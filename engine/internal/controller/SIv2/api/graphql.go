package api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/veritone/graphql"
)

const (
	defaultBaseURL                   = "https://api.veritone.com"
	defaultGraphQLEndpoint           = "/v3/graphql"
	defaultTimeout                   = "30s"
	defaultAssetTimeout              = "2m"
	defaultGetSignedWritableURLsSize = 50
	defaultMaxURLFetchAttempts       = 4
	defaultSignedURLExpirationBias   = 10 * time.Minute
)

var (
	errorUnabletoObtainNonExpiredSignedUrls = errors.New("unable to obtain non-expired signed urls")
)

type CoreAPIClient interface {
	CreateTDO(ctx context.Context, tdo TDO, taskID string) (*TDO, error)
	FetchTDO(ctx context.Context, tdoID string) (*TDO, error)
	UpdateTDO(ctx context.Context, tdo TDO, setStartStopTimes bool) (*TDO, error)
	CreateAsset(ctx context.Context, asset *Asset, r io.Reader) (*Asset, error)
	AddMediaSegment(ctx context.Context, asset *Asset) error
	CreateNextPipelineJob(ctx context.Context, jobID, tdoID string, startOffsetMS, endOffsetMS int) ([]Job, error)
	GetSignedURL(ctx context.Context) (string, string, error)
	ResetURLCache()
}

type Config struct {
	BaseURL         string `json:"baseURL,omitempty"`
	GraphQLEndpoint string `json:"graphQLEndpoint,omitempty"`
	Timeout         string `json:"timeout,omitempty"`
	AssetTimeout    string `json:"assetTimeout,omitempty"`
	Debug           bool   `json:"debug"`
	CorrelationID   string `json:"correlationID,omitempty"`
}

func (c *Config) defaults() {
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	if c.GraphQLEndpoint == "" {
		c.GraphQLEndpoint = defaultGraphQLEndpoint
	}
	if c.Timeout == "" {
		c.Timeout = defaultTimeout
	}
	if c.AssetTimeout == "" {
		c.AssetTimeout = defaultAssetTimeout
	}
}

type Request interface {
	Var(key string, value interface{})
	File(fieldname, filename string, r io.Reader)
}

type requestFactory func(q string) Request

var newRequest requestFactory = func(q string) Request {
	return graphql.NewRequest(q)
}

func beforeRetryHandler(req *http.Request, resp *http.Response, err error, num int) {
	if err != nil {
		log.Printf("Retrying (attempt %d) after err: %s -- request: %+v response: %+v", num, err, req, resp)
	} else {
		log.Printf("Retrying (attempt %d) after status: %s -- request: %+v response: %+v", num, resp.Status, req, resp)
	}
}

func NewGraphQLClient(config Config, token string) (CoreAPIClient, error) {
	config.defaults()
	endpoint := config.BaseURL + config.GraphQLEndpoint

	timeoutDuration, err := time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout value %q - %s", config.Timeout, err)
	}

	assetTimeoutDuration, err := time.ParseDuration(config.AssetTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid assetTimeout value %q - %s", config.AssetTimeout, err)
	}

	baseClient := graphql.NewClient(endpoint,
		withAuthAndCorrelationIDHeader(token, config.CorrelationID, timeoutDuration),
		graphql.WithDefaultExponentialRetryConfig(),
		graphql.WithBeforeRetryHandler(beforeRetryHandler))

	// asset client uses multi-part form uploads and a longer configured timeout
	assetClient := graphql.NewClient(endpoint,
		graphql.UseMultipartForm(),
		withAuthAndCorrelationIDHeader(token, config.CorrelationID, assetTimeoutDuration),
		graphql.WithDefaultExponentialRetryConfig(),
		graphql.WithBeforeRetryHandler(beforeRetryHandler))

	if config.Debug {
		baseClient.Log = func(s string) { log.Println(s) }
		assetClient.Log = baseClient.Log
	}

	return &graphQLClient{
		QueryRunner: baseClient,
		assetClient: assetClient,
	}, nil
}

func withAuthAndCorrelationIDHeader(token, correlationID string, timeout time.Duration) graphql.ClientOption {
	tr := &authHTTPTransport{
		Transport:     &http.Transport{},
		token:         token,
		correlationID: correlationID,
	}

	client := &http.Client{Transport: tr, Timeout: timeout}
	return graphql.WithHTTPClient(client)
}

type authHTTPTransport struct {
	*http.Transport
	token         string
	correlationID string
}

func (t *authHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("Veritone-Correlation-ID", t.correlationID)
	return t.Transport.RoundTrip(req)
}

type signedWritableURL struct {
	URL               string    `json:"url"`
	UnsignedURL       string    `json:"unsignedUrl"`
	ExpiresAtDateTime time.Time `json:"expiresAtDateTime"`
}

type QueryRunner interface {
	Run(ctx context.Context, req *graphql.Request, resp interface{}) error
}

type graphQLClient struct {
	QueryRunner
	assetClient  QueryRunner
	urlCache     []signedWritableURL
	urlCacheLock sync.Mutex
}

func (c *graphQLClient) CreateTDO(ctx context.Context, tdo TDO, taskID string) (*TDO, error) {
	req := newRequest(`
		mutation(
			$name: String!,
			$startDateTime: DateTime!,
			$stopDateTime: DateTime!,
			$status: String!,
			$isPublic: Boolean,
			$taskId: ID!,
			$details: JSONData
		) {
			createTDO(input: {
				name: $name,
				startDateTime: $startDateTime,
				stopDateTime: $stopDateTime,
				status: $status,
				isPublic: $isPublic,
				sourceData: {
					taskId: $taskId
				},
				details: $details
			}) {
				id
				name
				startDateTime
				stopDateTime
				status
				details
			}
		}`)

	req.Var("name", tdo.Name)
	req.Var("status", tdo.Status)
	req.Var("startDateTime", tdo.StartDateTime.Unix())
	req.Var("stopDateTime", tdo.StopDateTime.Unix())
	req.Var("taskId", taskID)
	req.Var("details", tdo.Details)

	var resp struct {
		Result *TDO `json:"createTDO"`
	}

	gqlReq, _ := req.(*graphql.Request)
	return resp.Result, c.Run(ctx, gqlReq, &resp)
}

func (c *graphQLClient) FetchTDO(ctx context.Context, tdoID string) (*TDO, error) {
	req := newRequest(`
		query($id: ID!) {
			temporalDataObject(id: $id) {
				id
				name
				startDateTime
				stopDateTime
				status
				details
			}
		}`)

	req.Var("id", tdoID)

	var resp struct {
		Result *TDO `json:"temporalDataObject"`
	}

	gqlReq, _ := req.(*graphql.Request)
	return resp.Result, c.Run(ctx, gqlReq, &resp)
}

func (c *graphQLClient) UpdateTDO(ctx context.Context, tdo TDO, setStartStopTimes bool) (*TDO, error) {
	qry := `
	mutation(
		$id: ID!,
		%s
		$status: String,
		$details: JSONData
		# $thumbnailUrl: String
		%s
	) {
		updateTDO(input: {
			id: $id,
			%s
			%s
			status: $status,
			details: $details
			# thumbnailUrl: $thumbnailUrl
		}) {
			id
			name
			startDateTime
			stopDateTime
			status
			details
			thumbnailUrl
		}
	}`

	replacements := []interface{}{"", "", "", ""}

	if setStartStopTimes {
		replacements[0] = `$startDateTime: DateTime,
		$stopDateTime: DateTime,`
		replacements[2] = `startDateTime: $startDateTime,
			stopDateTime: $stopDateTime,`
	}
	if tdo.PrimaryAsset.ID != "" {
		replacements[1] = `$primaryAssetId: ID!,
		$primaryAssetType: String!`
		replacements[3] = `primaryAsset: {
				id: $primaryAssetId,
				assetType: $primaryAssetType
			},`
	}

	qry = fmt.Sprintf(qry, replacements...)

	req := newRequest(qry)
	req.Var("id", tdo.ID)
	req.Var("status", tdo.Status)
	req.Var("startDateTime", tdo.StartDateTime.Unix())
	req.Var("stopDateTime", tdo.StopDateTime.Unix())
	req.Var("details", tdo.Details)
	// req.Var("thumbnailUrl", tdo.ThumbnailURL)
	req.Var("primaryAssetId", tdo.PrimaryAsset.ID)
	req.Var("primaryAssetType", tdo.PrimaryAsset.AssetType)

	var resp struct {
		Result *TDO `json:"updateTDO"`
	}

	gqlReq, _ := req.(*graphql.Request)
	return resp.Result, c.Run(ctx, gqlReq, &resp)
}

func (c *graphQLClient) AddMediaSegment(ctx context.Context, asset *Asset) error {
	if asset.URI == "" {
		return errors.New("URL cannot be empty")
	}

	req := newRequest(`
		mutation(
			$containerId: ID!,
			$url: String!,
			$details: JSONData!
		) {
			addMediaSegment(input: {
				containerId: $containerId,
				url: $url,
				details: $details
			}) {
				id
			}
		}`)

	req.Var("containerId", asset.ContainerID)
	req.Var("url", asset.URI)
	req.Var("details", asset.Details)

	var resp interface{}
	gqlReq, _ := req.(*graphql.Request)
	return c.Run(ctx, gqlReq, &resp)
}
func (c *graphQLClient) CreateAsset(ctx context.Context, asset *Asset, r io.Reader) (*Asset, error) {
	req := newRequest(`
		mutation(
			$containerId: ID!,
			$type: String!,
			$contentType: String,
			$name: String,
			$uri: String,
			$size: Int,
			$md5sum: String,
			$details: JSONData
		) {
			createAsset(input: {
				containerId: $containerId,
				type: $type,
				contentType: $contentType,
				name: $name,
				uri: $uri,
				fileData: {
					size: $size,
					md5sum: $md5sum
				},
				details: $details
			}) {
				id
				containerId
				type
				contentType
				name
				uri
				signedUri
				details
			}
		}`)

	req.Var("containerId", asset.ContainerID)
	req.Var("type", asset.Type)
	req.Var("contentType", asset.ContentType)
	req.Var("name", asset.Name)
	req.Var("size", asset.Size)
	req.Var("details", asset.Details)

	client := c.QueryRunner

	// if asset.URI is not empty, create asset by reference
	if asset.URI != "" {
		md5Hash, err := generateMD5(r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate MD5 hash for asset: %s", err)
		}

		req.Var("md5sum", md5Hash)
		req.Var("uri", asset.URI)
	} else {
		// upload the file via multi-part form
		req.File("file", asset.Name, r)
		client = c.assetClient
	}

	var resp struct {
		Result *Asset `json:"createAsset"`
	}

	gqlReq, _ := req.(*graphql.Request)
	return resp.Result, client.Run(ctx, gqlReq, &resp)
}

func (c *graphQLClient) CreateNextPipelineJob(ctx context.Context, jobID, tdoID string, startOffsetMS, endOffsetMS int) ([]Job, error) {
	req := newRequest(`
	mutation(
		$parentJobId: ID!,
		$tdoId: ID!,
		$startOffsetMs: Int,
		$endOffsetMs: Int
	) {
		createNextPipelineJobs(input: {
			parentJobId: $parentJobId,
			targetInfo: {
				targetId: $tdoId,
				startOffsetMs: $startOffsetMs,
				endOffsetMs: $endOffsetMs
			}
		}) {
			id
		}
	}`)

	req.Var("parentJobId", jobID)
	req.Var("tdoId", tdoID)
	req.Var("startOffsetMs", startOffsetMS)
	req.Var("endOffsetMs", endOffsetMS)

	var resp struct {
		Result []Job `json:"createNextPipelineJobs"`
	}

	gqlReq, _ := req.(*graphql.Request)
	return resp.Result, c.Run(ctx, gqlReq, &resp)
}

func generateMD5(r io.Reader) (string, error) {
	md5Hash := md5.New()
	if _, err := io.Copy(md5Hash, r); err != nil {
		return "", err
	}

	hashBytes := md5Hash.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)

	return hashString, nil
}

// GetSignedURL gets signed URL for uploading asset
// It could be S3 or other endpoints
func (c *graphQLClient) GetSignedURL(ctx context.Context) (string, string, error) {
	c.urlCacheLock.Lock()
	defer c.urlCacheLock.Unlock()
	expirationTime := time.Now().UTC().Add(defaultSignedURLExpirationBias)
	if len(c.urlCache) == 0 {
		newSignedURLSlice, err := c.fetchSignedURLs(ctx)
		if err != nil {
			return "", "", err
		}
		c.urlCache = newSignedURLSlice
	}
	// loop through current url cache. if non-expired one just return it. Otherwise throw away the whole cache
	urlFetchCount := 0
	for {
		var url *signedWritableURL
		url, c.urlCache = popFront(c.urlCache)
		if url == nil {
			urlFetchCount++
			if urlFetchCount > defaultMaxURLFetchAttempts {
				return "", "", errorUnabletoObtainNonExpiredSignedUrls
			}
			newSignedURLSlice, err := c.fetchSignedURLs(ctx)
			if err != nil {
				return "", "", err
			}
			c.urlCache = newSignedURLSlice
			continue
		}
		if url.ExpiresAtDateTime.After(expirationTime) {
			return url.URL, url.UnsignedURL, nil
		}
	}
}

func popFront(urls []signedWritableURL) (*signedWritableURL, []signedWritableURL) {
	if len(urls) == 0 {
		return nil, urls
	}
	e := urls[0]
	urls = urls[1:]
	return &e, urls
}

// ResetURLCache resets signed URL
func (c *graphQLClient) ResetURLCache() {
	c.urlCacheLock.Lock()
	defer c.urlCacheLock.Unlock()

	c.urlCache = nil
}

type fetchSignedURLsResponse struct {
	Result []signedWritableURL `json:"getSignedWritableUrls"`
}

func (c *graphQLClient) fetchSignedURLs(ctx context.Context) ([]signedWritableURL, error) {
	req := newRequest(`
		query ($number: Int!) {
			getSignedWritableUrls(number: $number) {
				url
				unsignedUrl
				expiresAtDateTime
			}
		}`)

	req.Var("number", defaultGetSignedWritableURLsSize)

	var resp fetchSignedURLsResponse

	gqlReq, _ := req.(*graphql.Request)
	err := c.Run(ctx, gqlReq, &resp)
	if err != nil || len(resp.Result) == 0 {
		if err == nil {
			err = errors.New("getSignedWritableUrls call returns empty result")
		}
		return nil, err
	}

	return resp.Result, nil
}
