package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/veritone/edge-stream-ingestor/api"
)

const (
	localOutputJSONFileName      = "aiwareconfig.json"
	defaultAssetUploadTimeout    = "1m"
	defaultAssetMaxAttempts      = 5
	defaultAssetRetryInterval    = 1.0
	defaultAssetMaxRetryInterval = 31.0

	amzReqIDField = "x-amz-request-id"
	amzIDField    = "x-amz-id-2"
)

func init() {
	// the mimes package does not have this extension
	mime.AddExtensionType(".msg", "application/vnd.ms-outlook")
}

type storeAssetFunc func(context.Context, *api.Asset, io.Reader) (string, error)

type AssetStorageConfig struct {
	LocalOutputDirectory string             `json:"localOutputDirectory,omitempty"`
	LocalOutputBaseURI   string             `json:"localOutputBaseUri,omitempty"`
	UsePresignedS3URLs   bool               `json:"usePresignedS3Urls"`
	Timeout              string             `json:"timeout,omitempty"`
	MaxAttempts          int                `json:"maxAttempts,omitempty"`
	RetryInterval        float64            `json:"retryInterval,omitempty"`
	MaxRetryInterval     float64            `json:"maxRetryInterval,omitempty"`
	MinioStorage         MinioStorageConfig `json:"portableEdgeMedia,omitempty"`
}

//Config to store the URL based Minio server URL, Minio Media Segment Bucket as supplied by SEM
type MinioStorageConfig struct {
	MinioServerURLIPBased string `json:"minioServerURLIPBased,omitempty"`
	MediaSegmentBucket    string `json:"minioMediaSegmentBucket,omitempty"`
	MinioServer           string `json:"minioServer,omitempty"`
}

func (c *AssetStorageConfig) defaults() {
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultAssetMaxAttempts
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = defaultAssetRetryInterval
	}
	if c.MaxRetryInterval == 0 {
		c.MaxRetryInterval = defaultAssetMaxRetryInterval
	}
	if c.Timeout == "" {
		c.Timeout = defaultAssetUploadTimeout
	}
}

func InitAssetStore(config AssetStorageConfig, coreAPIClient api.CoreAPIClient, tdo *api.TDO) (*AssetStore, error) {
	var err error
	config.defaults()

	httpClient := &http.Client{
		Transport: http.DefaultClient.Transport,
	}
	httpClient.Timeout, err = time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, fmt.Errorf("invalid timeout specified %q: %s", config.Timeout, err)
	}

	storeAssetFn, err := selectAssetStorage(config, tdo.ID, httpClient, coreAPIClient)
	if err != nil {
		return nil, err
	}

	return &AssetStore{
		config:        config,
		coreAPIClient: coreAPIClient,
		storeAssetFn:  storeAssetFn,
	}, nil
}

type AssetStore struct {
	config        AssetStorageConfig
	coreAPIClient api.CoreAPIClient
	storeAssetFn  storeAssetFunc
}

func (as *AssetStore) StoreAsset(ctx context.Context, asset *api.Asset, r io.Reader, isSegment bool) (*api.Asset, error) {
	var (
		createdAsset *api.Asset
		err          error
	)

	if asset.Size <= 0 {
		return nil, errors.New("asset size must be greater than 0")
	}

	if as.storeAssetFn == nil {
		// upload directly via graphql multi-part form request - the client will handle retries
		createdAsset, err = as.coreAPIClient.CreateAsset(ctx, asset, r)
		if err != nil {
			return nil, err
		}
	} else {
		// reader must be reset each time the write operation is attempted, so Seek is required
		rs, ok := r.(io.ReadSeeker)
		if !ok {
			// if the reader is not seekable, read the contents into seekable buffer
			assetBytes, err := ioutil.ReadAll(r)
			if err != nil {
				return nil, err
			}

			rs = bytes.NewReader(assetBytes)
		}

		attempt, maxAttempts := 1, as.config.MaxAttempts

		for ; attempt <= maxAttempts; attempt++ {
			// reset seek head
			if _, err := rs.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}

			// store the asset then create an asset by reference
			asset.URI, err = as.storeAssetFn(ctx, asset, ioutil.NopCloser(rs))
			if !shouldRetry(err) {
				break
			}

			// if err is retryable, sleep then try again
			time.Sleep(retryInterval(as.config, attempt))
		}

		if err != nil {
			if attempt > 1 {
				err = fmt.Errorf("tried %d times and failed with error: %s", attempt-1, err)
			}
			return nil, err
		}

		if isSegment {
			createdAsset = asset
			createdAsset.ID = asset.URI
			if err := as.coreAPIClient.AddMediaSegment(ctx, asset); err != nil {
				return nil, err
			}
		} else {
			// reset reader again (for MD5 hash)
			if _, err := rs.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}

			createdAsset, err = as.coreAPIClient.CreateAsset(ctx, asset, rs)
			if err != nil {
				return nil, err
			}
		}
	}

	// api query does not return size
	createdAsset.Size = asset.Size

	return createdAsset, nil
}

func retryInterval(c AssetStorageConfig, attempt int) time.Duration {
	i := math.Min(float64(attempt)*float64(c.RetryInterval), float64(c.MaxRetryInterval))
	return time.Duration(i)
}

func useLocalAssetStorage(outputDirectory string, localURIBase *url.URL) storeAssetFunc {
	return func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		fileName := asset.Name

		// move source file under newly created directory
		dest := filepath.Join(outputDirectory, fileName)

		df, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return "", fmt.Errorf("failed to create file in local directory: %v", err)
		}
		defer df.Close()

		written, err := io.Copy(df, r)
		if err != nil {
			return "", fmt.Errorf("An error occurred while writing asset to local file: %s", err)
		}

		Logger.Printf("Successfully copied asset (%.02f kb) to local output directory: %s", float64(written)/1024, dest)

		localURI := *localURIBase
		localURI.Path = path.Join(localURI.Path, fileName)

		return localURI.String(), nil
	}
}

func useS3AssetStorage(graphQLClient api.CoreAPIClient, httpClient *http.Client) storeAssetFunc {
	return func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		// hack: create image assets directly via graphql since pregenerated s3 URLs have no file extension
		if asset.Type == api.AssetTypeThumbnail || strings.HasPrefix(asset.ContentType, "image/") {
			return "", nil
		}

		signedURL, unsignedURL, err := graphQLClient.GetSignedURL(ctx)
		if err != nil {
			return "", err
		} else if unsignedURL == "" {
			return "", errors.New("S3 URL is empty")
		}

		err = uploadContentToURL(ctx, httpClient, r, signedURL, asset.ContentType, asset.Size)
		// hack 2: if S3 returns 400 Bad Request (typically "The provided token has expired"), reset the
		// URL cache and return a retryable error (vtn-19177)
		if err != nil && strings.Contains(err.Error(), http.StatusText(http.StatusBadRequest)) {
			// Delete cache
			graphQLClient.ResetURLCache()
			return "", retryableError{err}
		}

		return unsignedURL, err
	}
}

//UsingMinioAssetStorage stores the media assets into a minio bucket. This function is used only by Portable Edge.
//Minio server and bucket config has been passed in through the PortableEdgeMediaSegmentConfig
//The minio server url is passed-in in 2 flavours. One machine IP based and another one hostname based.
//Hostname name based URL is used for HTTP upload. Ex. minioBucketHostNameURL - http://minio-1:9101/portableedge/assets/tdoid/094fa3b9-4963-4583-91cc-cd8bacc0e278
//IP based URL is sent to GraphQL to be stored as Asset URL for media play back. Ex. minioBucketIPURL - http://54.174.79.89:9101/portableedge/assets/tdoid/094fa3b9-4963-4583-91cc-cd8bacc0e278
//IP based URL is the SaaS version's signed URI.
func useMinioAssetStorage(httpClient *http.Client, tdoID string, minioStorageConfig MinioStorageConfig) storeAssetFunc {
	return func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		uuid := uuid.New()
		//Ex. minioBucketHostNameURL - http://minio-1:9101/portableedge/assets/tdoid/094fa3b9-4963-4583-91cc-cd8bacc0e278
		minioBucketHostNameURL := fmt.Sprintf("%s/%s/assets/%s/%s", minioStorageConfig.MinioServer, minioStorageConfig.MediaSegmentBucket, tdoID, uuid)
		//Ex. minioBucketIPURL - http://54.174.79.89:9101/portableedge/assets/tdoid/094fa3b9-4963-4583-91cc-cd8bacc0e278
		minioBucketIPURL := fmt.Sprintf("%s/%s/assets/%s/%s", minioStorageConfig.MinioServerURLIPBased, minioStorageConfig.MediaSegmentBucket, tdoID, uuid)

		// determine file extension
		fileExts, err := mime.ExtensionsByType(asset.ContentType)
		if err != nil {
			Logger.Printf("failed to determine file extension for asset with MIME type %q: %s", asset.ContentType, err)
		}
		if len(fileExts) > 0 {
			minioBucketHostNameURL += fileExts[0]
			minioBucketIPURL += fileExts[0]
		}

		return minioBucketIPURL, uploadContentToURL(ctx, httpClient, r, minioBucketHostNameURL, asset.ContentType, asset.Size)
	}
}

// uploadContentToURL uploads asset to signed URL endpoint
// it could be a S3 object or other endpoints
func uploadContentToURL(ctx context.Context, httpClient *http.Client, r io.Reader, url, contentType string, contentLength int64) error {
	httpReq, err := http.NewRequest("PUT", url, r)
	if err != nil {
		return fmt.Errorf("failed to prepare request: %s", err)
	}

	httpReq.ContentLength = contentLength
	httpReq.Header.Set("Content-Type", contentType)
	httpReq.Header.Set("x-ms-blob-type", "BlockBlob") // required for the Azure storage

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return retryableError{err} // always retry HTTP errors
	}
	defer resp.Body.Close()

	if status := resp.StatusCode; status != 200 && status != 201 { // Azure returns HTTP code 201
		respBytes, _ := ioutil.ReadAll(resp.Body)

		Logger.Printf("HTTP upload to %q (ContentLength: %d, ContentType: %s, Amazon request pair(%s|%s)) failed with status %q: %s",
			url, contentLength, contentType, resp.Header.Get(amzReqIDField), resp.Header.Get(amzIDField), resp.Status, string(respBytes))

		if (status >= 500 && status <= 599) || status == http.StatusTooManyRequests {
			return retryableErrorf("HTTP upload returned %d %s status", status, resp.Status)
		}

		return fmt.Errorf("HTTP upload returned %d %s status", status, resp.Status)
	}

	return nil
}

func selectAssetStorage(cfg AssetStorageConfig, tdoID string, httpClient *http.Client, coreAPIClient api.CoreAPIClient) (storeAssetFunc, error) {
	if cfg.LocalOutputDirectory != "" {
		subDirPath := filepath.Join(time.Now().Format("20060102"), tdoID)
		destPath := filepath.Join(cfg.LocalOutputDirectory, subDirPath)

		// create path under local output directory
		if err := os.MkdirAll(destPath, 0777); err != nil {
			return nil, err
		}

		localURIBase, err := url.Parse(cfg.LocalOutputBaseURI)
		if err != nil {
			return nil, err
		}
		localURIBase.Path = path.Join(localURIBase.Path, subDirPath)

		Logger.Printf("Using local output directory %q with base URI %q", destPath, localURIBase)
		return useLocalAssetStorage(destPath, localURIBase), nil
	} else if cfg.MinioStorage.MediaSegmentBucket != "" {
		Logger.Printf("Using Minio Media Segment Bucket: %s", cfg.MinioStorage.MediaSegmentBucket)
		return useMinioAssetStorage(httpClient, tdoID, cfg.MinioStorage), nil
	} else if cfg.UsePresignedS3URLs {
		Logger.Println("Using pre-signed S3 URLs for asset storage")
		return useS3AssetStorage(coreAPIClient, httpClient), nil
	}

	// use default asset storage strategy (graphQL multi-part form upload)
	return nil, nil
}

type retryableError struct {
	error
}

func retryableErrorf(format string, args ...interface{}) error {
	return retryableError{fmt.Errorf(format, args...)}
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	if _, ok := err.(retryableError); ok {
		return true
	}

	netErr, ok := err.(net.Error)
	if ok && netErr.Timeout() {
		return true
	}

	return false
}
