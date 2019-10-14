package api

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	apiMocks "github.com/veritone/edge-stream-ingestor/api/mocks/mocks_internal"
)

type graphqlTestSuite struct {
	suite.Suite
	defaultQueryRunner *apiMocks.QueryRunner
	assetQueryRunner   *apiMocks.QueryRunner
	client             *graphQLClient
	req                *apiMocks.Request
}

func (t *graphqlTestSuite) SetupTest() {
	t.defaultQueryRunner = new(apiMocks.QueryRunner)
	t.assetQueryRunner = new(apiMocks.QueryRunner)

	t.client = &graphQLClient{
		QueryRunner: t.defaultQueryRunner,
		assetClient: t.assetQueryRunner,
	}

	t.client.urlCache = []signedWritableURL{{
		URL:               "https://some.uri/1?signed",
		UnsignedURL:       "https://some.uri/1",
		ExpiresAtDateTime: time.Now().Add(time.Hour),
	}, {
		URL:               "https://some.uri/2?signed",
		UnsignedURL:       "https://some.uri/2",
		ExpiresAtDateTime: time.Now().Add(time.Hour),
	}, {
		URL:               "https://some.uri/3?signed",
		UnsignedURL:       "https://some.uri/3",
		ExpiresAtDateTime: time.Now().Add(time.Hour),
	}, {
		URL:               "https://some.uri/4?signed",
		UnsignedURL:       "https://some.uri/4",
		ExpiresAtDateTime: time.Now().Add(time.Hour),
	}, {
		URL:               "https://some.uri/5?signed",
		UnsignedURL:       "https://some.uri/5",
		ExpiresAtDateTime: time.Now().Add(time.Hour),
	}}

	t.req = new(apiMocks.Request)
	t.req.On("Var", mock.AnythingOfType("string"), mock.Anything).Return()
	t.req.On("File", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("*bytes.Reader")).Return()
	newRequest = func(q string) Request {
		return t.req
	}
}

func (t *graphqlTestSuite) TestCreateAssetByReference() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	content := []byte("1234567890!@#$%^&*()")
	asset := &Asset{
		URI:         "https://some.uri/12345.mp4",
		ContainerID: "tdo1",
		Type:        AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
		Size:        int64(len(content)),
		Details: map[string]interface{}{
			"width":  1280,
			"height": 720,
		},
	}

	expectedVars := map[string]interface{}{
		"containerId": asset.ContainerID,
		"type":        asset.Type,
		"contentType": asset.ContentType,
		"name":        asset.Name,
		"size":        asset.Size,
		"details":     asset.Details,
		"uri":         asset.URI,
		"md5sum":      "68a32ac5dbc2a9972fefa61a3ed0e4a5",
	}

	_, err := t.client.CreateAsset(ctx, asset, bytes.NewReader(content))
	if t.NoError(err) {
		if t.defaultQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			actualVars := make(map[string]interface{})
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					actualVars[key] = call.Arguments.Get(1)
				}
			}

			t.Equal(expectedVars, actualVars)
		}

		t.req.AssertNotCalled(t.T(), "File", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("io.Reader"))
	}
}

func (t *graphqlTestSuite) TestCreateAssetMultipart() {
	ctx := context.Background()
	t.assetQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	content := []byte("1234567890!@#$%^&*()")
	asset := &Asset{
		ContainerID: "tdo1",
		Type:        AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
		Size:        int64(len(content)),
		Details: map[string]interface{}{
			"width":  1280,
			"height": 720,
		},
	}

	expectedVars := map[string]interface{}{
		"containerId": asset.ContainerID,
		"type":        asset.Type,
		"contentType": asset.ContentType,
		"name":        asset.Name,
		"size":        asset.Size,
		"details":     asset.Details,
	}

	r := bytes.NewReader(content)
	_, err := t.client.CreateAsset(ctx, asset, r)
	if t.NoError(err) {
		if t.assetQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			actualVars := make(map[string]interface{})
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					actualVars[key] = call.Arguments.Get(1)
				}
			}

			t.Equal(expectedVars, actualVars)
		}

		t.req.AssertCalled(t.T(), "File", "file", asset.Name, r)
	}
}

func (t *graphqlTestSuite) TestGetSignedURL() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Run(func(args mock.Arguments) {
		res := args.Get(2).(*fetchSignedURLsResponse)
		res.Result = []signedWritableURL{{
			URL:               "https://some.uri/6?signed",
			UnsignedURL:       "https://some.uri/6",
			ExpiresAtDateTime: time.Now().Add(time.Hour),
		}, {
			URL:               "https://some.uri/7?signed",
			UnsignedURL:       "https://some.uri/7",
			ExpiresAtDateTime: time.Now().Add(time.Hour),
		}}
	}).Return(nil)

	for i := 1; i < 7; i++ {
		signedURL, unsignedURL, err := t.client.GetSignedURL(ctx)
		if t.NoError(err) {
			t.Equal(fmt.Sprintf("https://some.uri/%d?signed", i), signedURL)
			t.Equal(fmt.Sprintf("https://some.uri/%d", i), unsignedURL)
		}
	}

	t.Len(t.client.urlCache, 1)
}

func (t *graphqlTestSuite) TestGetSignedURLExpired() {
	ctx := context.Background()
	t.client.urlCache = []signedWritableURL{}
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Run(func(args mock.Arguments) {
		res := args.Get(2).(*fetchSignedURLsResponse)
		res.Result = []signedWritableURL{{
			URL:               "https://some.uri/6?signed",
			UnsignedURL:       "https://some.uri/6",
			ExpiresAtDateTime: time.Now().Add(defaultSignedURLExpirationBias).Add(-time.Millisecond * 10),
		}, {
			URL:               "https://some.uri/7?signed",
			UnsignedURL:       "https://some.uri/7",
			ExpiresAtDateTime: time.Now().Add(time.Hour),
		}}
	}).Return(nil)

	signedURL, unsignedURL, err := t.client.GetSignedURL(ctx)

	// First url expected to be skipped as it has expired.
	if t.NoError(err) {
		t.Equal("https://some.uri/7?signed", signedURL)
		t.Equal("https://some.uri/7", unsignedURL)
	}
	t.Len(t.client.urlCache, 0)
}

func (t *graphqlTestSuite) TestGetSignedURLExpiredMaximumAllowedTimes() {
	ctx := context.Background()
	t.client.urlCache = []signedWritableURL{}
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Run(func(args mock.Arguments) {
		res := args.Get(2).(*fetchSignedURLsResponse)
		res.Result = []signedWritableURL{{
			URL:               "https://some.uri/6?signed",
			UnsignedURL:       "https://some.uri/6",
			ExpiresAtDateTime: time.Now().Add(defaultSignedURLExpirationBias).Add(-time.Millisecond * 10),
		},
		}
	}).Return(nil)

	signedURL, unsignedURL, err := t.client.GetSignedURL(ctx)

	t.Empty(signedURL)
	t.Empty(unsignedURL)
	t.Equal(err, errorUnabletoObtainNonExpiredSignedUrls)
	t.Len(t.client.urlCache, 0)
}

func (t *graphqlTestSuite) TestCreateNextPipelineJob() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	jobID := "job1"
	tdoID := "tdo1"
	startOffsetMS := 9
	endOffsetMS := 99

	expectedVars := map[string]interface{}{
		"parentJobId":   jobID,
		"tdoId":         tdoID,
		"startOffsetMs": startOffsetMS,
		"endOffsetMs":   endOffsetMS,
	}

	_, err := t.client.CreateNextPipelineJob(ctx, jobID, tdoID, startOffsetMS, endOffsetMS)
	if t.NoError(err) {
		if t.defaultQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			actualVars := make(map[string]interface{})
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					actualVars[key] = call.Arguments.Get(1)
				}
			}

			t.Equal(expectedVars, actualVars)
		}
	}
}

func (t *graphqlTestSuite) TestCreateTDO() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	name := "name1"
	status := TDOStatus("status1")
	details := map[string]interface{}{"k": "v"}
	taskID := "task1"

	expectedVars := map[string]interface{}{
		"name":    name,
		"status":  status,
		"details": details,
		"taskId":  taskID,
	}

	tdo := TDO{
		Name:    name,
		Status:  status,
		Details: details,
	}

	_, err := t.client.CreateTDO(ctx, tdo, taskID)
	if t.NoError(err) {
		if t.defaultQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			actualVars := make(map[string]interface{})
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					if key == "name" || key == "status" || key == "details" || key == "taskId" {
						actualVars[key] = call.Arguments.Get(1)
					}
				}
			}

			t.Equal(expectedVars, actualVars)
		}
	}
}

func (t *graphqlTestSuite) TestFetchTDO() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	jobID := "job1"

	_, err := t.client.FetchTDO(ctx, jobID)
	if t.NoError(err) {
		if t.defaultQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					if key == "id" {
						t.Equal(jobID, call.Arguments.Get(1))
						return
					}
				}
			}
		}
	}
}

func (t *graphqlTestSuite) TestUpdateTDO() {
	ctx := context.Background()
	t.defaultQueryRunner.On("Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything).Return(nil)

	id := "id1"
	status := TDOStatus("status1")
	details := map[string]interface{}{"k": "v"}
	assetID := "assetId1"
	assetType := "assetType1"

	expectedVars := map[string]interface{}{
		"id":               id,
		"status":           status,
		"details":          details,
		"primaryAssetId":   assetID,
		"primaryAssetType": assetType,
	}

	tdo := TDO{
		ID:      id,
		Status:  status,
		Details: details,
		PrimaryAsset: TDOPrimaryAsset{
			ID:        assetID,
			AssetType: assetType,
		},
	}

	_, err := t.client.UpdateTDO(ctx, tdo, true)
	if t.NoError(err) {
		if t.defaultQueryRunner.AssertCalled(t.T(), "Run", ctx, mock.AnythingOfType("*graphql.Request"), mock.Anything) {
			actualVars := make(map[string]interface{})
			for _, call := range t.req.Calls {
				if call.Method == "Var" {
					key := call.Arguments.String(0)
					if !strings.Contains(key, "Time") {
						actualVars[key] = call.Arguments.Get(1)
					}
				}
			}

			t.Equal(expectedVars, actualVars)
		}
	}
}
