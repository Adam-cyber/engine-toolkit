package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/veritone/edge-stream-ingestor/api"
	apiMocks "github.com/veritone/edge-stream-ingestor/api/mocks"
	ingestMocks "github.com/veritone/edge-stream-ingestor/ingest/mocks/mocks_internal"
)

type storageTestSuite struct {
	suite.Suite
	httpTransport *ingestMocks.RoundTripper
	graphQLClient *apiMocks.CoreAPIClient
	assetStore    *AssetStore
}

func (t *storageTestSuite) SetupTest() {
	t.httpTransport = new(ingestMocks.RoundTripper)
	t.graphQLClient = new(apiMocks.CoreAPIClient)
	t.assetStore = &AssetStore{
		coreAPIClient: t.graphQLClient,
	}
	t.assetStore.config.defaults()
}

func (t *storageTestSuite) TestStoreAssetDirectUpload() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		ContainerID: "tdo1",
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
		Size:        int64(len(content)),
		Details: map[string]interface{}{
			"width":  1280,
			"height": 720,
		},
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Return(&api.Asset{
		ID:          "1234",
		ContainerID: asset.ContainerID,
		Type:        asset.Type,
		ContentType: asset.ContentType,
		Name:        asset.Name,
		Details:     asset.Details,
	}, nil)

	createdAsset, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewReader(content), false)
	if t.NoError(err) {
		t.Equal("1234", createdAsset.ID)
		t.Equal(int64(len(content)), createdAsset.Size)
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1) {
			readerArg := t.graphQLClient.Calls[0].Arguments.Get(2).(io.Reader)
			readerBytes, _ := ioutil.ReadAll(readerArg)
			t.Equal(content, readerBytes)
		}
	}
}

func (t *storageTestSuite) TestStoreAssetFromNonSeekableBuffer() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		ContainerID: "tdo1",
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
		Size:        int64(len(content)),
		Details: map[string]interface{}{
			"width":  1280,
			"height": 720,
		},
	}

	gqlReqBuf := new(bytes.Buffer)
	storeAssetBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "file://1.2.3.4/file.mp4", nil
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		gqlReqBuf.Write(b)
	}).Return(&api.Asset{
		ID:          "1234",
		ContainerID: asset.ContainerID,
		Type:        asset.Type,
		ContentType: asset.ContentType,
		Name:        asset.Name,
		Details:     asset.Details,
	}, nil)

	createdAsset, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.NoError(err) {
		t.Equal("1234", createdAsset.ID)
		t.Equal(int64(len(content)), createdAsset.Size)
		t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1)

		bufContents, _ := ioutil.ReadAll(gqlReqBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))

		bufContents, _ = ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetFromSeekableBuffer() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		ContainerID: "tdo1",
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
		Size:        int64(len(content)),
		Details: map[string]interface{}{
			"width":  1280,
			"height": 720,
		},
	}

	gqlReqBuf := new(bytes.Buffer)
	storeAssetBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "file://1.2.3.4/file.mp4", nil
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		gqlReqBuf.Write(b)
	}).Return(&api.Asset{
		ID:          "1234",
		ContainerID: asset.ContainerID,
		Type:        asset.Type,
		ContentType: asset.ContentType,
		Name:        asset.Name,
		Details:     asset.Details,
	}, nil)

	createdAsset, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewReader(content), false)
	if t.NoError(err) {
		t.Equal("1234", createdAsset.ID)
		t.Equal(int64(len(content)), createdAsset.Size)
		t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1)

		bufContents, _ := ioutil.ReadAll(gqlReqBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))

		bufContents, _ = ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetZeroSize() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		ContainerID: "tdo1",
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Name:        "video.mp4",
	}

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewReader(content), false)
	t.Error(err)
}

func (t *storageTestSuite) TestStoreAssetRetryAndFailNonSeekable() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}

	storeAssetBuf := new(bytes.Buffer)
	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "", retryableErrorf("temporary failure")
	}

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.Error(err) {
		t.Contains(err.Error(), "temporary failure")
		bufContents, _ := ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 100)
		t.Equal("1234567890!@#$%^&*()1234567890!@#$%^&*()1234567890!@#$%^&*()1234567890!@#$%^&*()1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetNonRetryableError() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}

	storeAssetBuf := new(bytes.Buffer)
	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "", errors.New("hard fail")
	}

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.Error(err) {
		t.Contains(err.Error(), "hard fail")
		bufContents, _ := ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetRetryOnce() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}

	gqlRefBuf := new(bytes.Buffer)
	storeAssetBuf := new(bytes.Buffer)
	callNum := 0
	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)

		callNum++
		if callNum == 1 {
			return "", retryableErrorf("retry me")
		}

		return "http://1.2.3.4/file.mp4", nil
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		gqlRefBuf.Write(b)
	}).Return(&api.Asset{
		ID: "1234",
	}, nil)

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.NoError(err) {
		t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1)
		bufContents, _ := ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 40)
		t.Equal("1234567890!@#$%^&*()1234567890!@#$%^&*()", string(bufContents))

		bufContents, _ = ioutil.ReadAll(gqlRefBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetStorageFn() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}
	gqlReqBuf := new(bytes.Buffer)
	storeAssetBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "file://1.2.3.4/file.mp4", nil
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		gqlReqBuf.Write(b)
	}).Return(&api.Asset{
		ID:          "1234",
		ContainerID: asset.ContainerID,
		Type:        asset.Type,
		ContentType: asset.ContentType,
		Name:        asset.Name,
		Details:     asset.Details,
	}, nil)

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.NoError(err) {
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1) {
			assetArg := t.graphQLClient.Calls[0].Arguments.Get(1).(*api.Asset)
			t.Equal("file://1.2.3.4/file.mp4", assetArg.URI)
		}

		bufContents, _ := ioutil.ReadAll(gqlReqBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))

		bufContents, _ = ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetStorageFnError() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}
	storeAssetBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "", errors.New("HTTP upload returned 400 Bad Request status")
	}

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.Error(err) {
		t.Contains(err.Error(), "HTTP upload returned 400 Bad Request status")
		bufContents, _ := ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetStorageFnSkipStorage() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Size: int64(len(content)),
	}
	gqlReqBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		// this emulates the use case when we don't want to store the asset in a pre-signed S3 URI and use gql instead
		return "", nil
	}

	t.graphQLClient.On("CreateAsset", ctx, asset, mock.AnythingOfType("*bytes.Reader")).Run(func(args mock.Arguments) {
		r := args.Get(2).(io.Reader)
		b, _ := ioutil.ReadAll(r)
		gqlReqBuf.Write(b)
	}).Return(&api.Asset{
		ID:          "1234",
		ContainerID: asset.ContainerID,
		Type:        asset.Type,
		ContentType: asset.ContentType,
		Name:        asset.Name,
		Details:     asset.Details,
	}, nil)

	_, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), false)
	if t.NoError(err) {
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "CreateAsset", 1) {
			assetArg := t.graphQLClient.Calls[0].Arguments.Get(1).(*api.Asset)
			t.Equal("", assetArg.URI)
		}

		bufContents, _ := ioutil.ReadAll(gqlReqBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
	}
}

func (t *storageTestSuite) TestStoreAssetMediaSegment() {
	ctx := context.Background()
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{Size: 20}
	storeAssetBuf := new(bytes.Buffer)

	t.assetStore.storeAssetFn = func(ctx context.Context, asset *api.Asset, r io.Reader) (string, error) {
		b, _ := ioutil.ReadAll(r)
		storeAssetBuf.Write(b)
		return "file://1.2.3.4/file.mp4", nil
	}

	t.graphQLClient.On("AddMediaSegment", ctx, asset).Return(nil)

	createdAsset, err := t.assetStore.StoreAsset(ctx, asset, bytes.NewBuffer(content), true)
	if t.NoError(err) {
		if t.graphQLClient.AssertNumberOfCalls(t.T(), "AddMediaSegment", 1) {
			assetArg := t.graphQLClient.Calls[0].Arguments.Get(1).(*api.Asset)
			t.Equal("file://1.2.3.4/file.mp4", assetArg.URI)
		}

		bufContents, _ := ioutil.ReadAll(storeAssetBuf)
		t.Len(bufContents, 20)
		t.Equal("1234567890!@#$%^&*()", string(bufContents))
		t.Equal(int64(20), createdAsset.Size)
	}
}

func (t *storageTestSuite) TestUploadContentToURL() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "OK",
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.NoError(err) {
		if t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1) {
			req := t.httpTransport.Calls[0].Arguments.Get(0).(*http.Request)
			t.Equal(int64(len(content)), req.ContentLength)
			t.Equal("video/mp4", req.Header.Get("Content-Type"))
		}
	}
}

func (t *storageTestSuite) TestUploadContentToAzureURL() {
	ctx := context.Background()

	mockCall := t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request"))

	mockCall.Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()

		if req.Header.Get("x-ms-blob-type") == "BlockBlob" {
			mockCall.Return(&http.Response{
				Status:     "OK",
				StatusCode: 201,
				Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
			}, nil)
		} else {
			mockCall.Return(&http.Response{
				Status:     "Bad Request",
				StatusCode: 400,
				Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
			}, errors.New("Bad Request to Azure Storage"))
		}
	})

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.NoError(err) {
		if t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1) {
			req := t.httpTransport.Calls[0].Arguments.Get(0).(*http.Request)
			t.Equal(int64(len(content)), req.ContentLength)
			t.Equal("video/mp4", req.Header.Get("Content-Type"))
		}
	}
}

func (t *storageTestSuite) TestUploadContentToURLRetryableStatus() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "Service Unavailable",
		StatusCode: 502,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.Error(err) {
		t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1)
		t.True(shouldRetry(err))
	}
}

func (t *storageTestSuite) TestUploadContentToURLRetryableError() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(nil, errors.New("EOF"))

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.Error(err) {
		t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1)
		t.True(shouldRetry(err))
	}
}

// func (t *storageTestSuite) TestUploadContentToURLRetryableError2() {
// 	ctx := context.Background()

// 	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
// 		req := args.Get(0).(*http.Request)
// 		ioutil.ReadAll(req.Body)
// 		req.Body.Close()
// 	}).Return(nil, errors.New("net/http: HTTP/1.x transport connection broken: write tcp 172.17.0.89:48634->52.216.106.206:443: write: connection reset by peer"))

// 	content := []byte("1234567890!@#$%^&*()")
// 	httpClient := &http.Client{Transport: t.httpTransport}

// 	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
// 	if t.Error(err) {
// 		t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1)
// 		t.True(shouldRetry(err))
// 	}
// }

func (t *storageTestSuite) TestUploadContentToURLNonRetryableError() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "Unauthorized",
		StatusCode: 403,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.Error(err) {
		t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1)
		t.False(shouldRetry(err))
	}
}

func (t *storageTestSuite) TestUploadContentToURLNonRetryableStatus() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "Access Denied",
		StatusCode: 403,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	content := []byte("1234567890!@#$%^&*()")
	httpClient := &http.Client{Transport: t.httpTransport}

	err := uploadContentToURL(ctx, httpClient, bytes.NewReader(content), "test.url", "video/mp4", int64(len(content)))
	if t.Error(err) {
		t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1)
		t.False(shouldRetry(err))
	}
}

func (t *storageTestSuite) TestS3AssetStorage() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "OK",
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	t.graphQLClient.On("GetSignedURL", ctx).Return("some.url?signed", "some.url", nil)

	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Size:        int64(len(content)),
	}
	httpClient := &http.Client{Transport: t.httpTransport}

	storeAssetFn := useS3AssetStorage(t.graphQLClient, httpClient)
	url, err := storeAssetFn(ctx, asset, bytes.NewReader(content))
	if t.NoError(err) {
		t.Equal("some.url", url)
		if t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1) {
			req := t.httpTransport.Calls[0].Arguments.Get(0).(*http.Request)
			t.Equal(asset.Size, req.ContentLength)
			t.Equal("some.url", req.URL.Path)
			t.Equal("signed", req.URL.RawQuery)
		}
	}
}

func (t *storageTestSuite) TestS3AssetStorageNon200Response() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "Too Many Requests",
		StatusCode: 429,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte(`{}`))),
	}, nil)

	t.graphQLClient.On("GetSignedURL", ctx).Return("some.url?signed", "some.url", nil)

	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Size:        int64(len(content)),
	}
	httpClient := &http.Client{Transport: t.httpTransport}

	storeAssetFn := useS3AssetStorage(t.graphQLClient, httpClient)
	_, err := storeAssetFn(ctx, asset, bytes.NewReader(content))
	if t.Error(err) {
		t.Contains(err.Error(), "429 Too Many Requests")
		t.True(shouldRetry(err))

		if t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1) {
			req := t.httpTransport.Calls[0].Arguments.Get(0).(*http.Request)
			t.Equal(asset.Size, req.ContentLength)
			t.Equal("some.url", req.URL.Path)
			t.Equal("signed", req.URL.RawQuery)
		}
	}
}

func (t *storageTestSuite) TestS3AssetStorage400Response() {
	ctx := context.Background()

	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "Bad Request",
		StatusCode: 400,
		Body: ioutil.NopCloser(bytes.NewReader([]byte(`<?xml version="1.0" encoding="UTF-8"?>
		<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message><Token-0>FQoGZXIvYXdzEAIaDBWAegzE/gYY7x+esCKoBJof+Qmt5Xdg6ZwnvdyIeDBd4QkMLaJpsIE6CsEbR1w6B7BaThXTS07rx6wVGHcZMMMl8lYFrv4hvFFeAu+O9djzDWNP6aetIgJIJyOsyrjV2KuMdS299hqbmLQHtITLe9FhvegCTksIQDhsYWZ20WZmCmRTr8U7lCJRFiO5PxShPRPgbYquLA4p+hZrRRse3/xwCxbaLPlo7NWaXLc17x3a8aJ6oZS9DhaQHKh75AGDnNayy5NrFvCRE+4XUrlIDIum8jy8Suc074PQS04+WRHgbJ4yGXDnUpnI52gk45MPNCKuNYaRO23+kYUT5IPSMhQpc3uxTt/UvCDFFJDVFcBkQEGf2YYDERuoGpF6uUC2ez4vycUHSgr7i2UIcZbMmXjGuR0zxWG2wpcaPHEcBrkCIb3gVk8LXE7tJYryzR6qs9n8NcZ+d14ALxtzLdSpNDa/khyB2j1xcKYNxZqXOdNr9Mn1Ujo2Wp9wQuFbDfeZhEVz4uT7Eziq+GhlOWr+rl2o4DITuPtSkwuVuSLWuNA4t269azdbcOM+WbTxAuTsmaetVmi2ybSUkR34x8OLgTFMrQVQR+RGhulcOSPIT78dq1Kovejtgg6+8EiP/whoX0iolUPSICvmx4gikWcjZn6cWEqApQmYdSsq9iSTotvhYUGnnXi3tJ2oQhUjGq7ztQb16LabiHWmdm4p6CErRKIO55rrQu7seBo92Lqjp+zbRRTgr7xlpSjFsITiBQ==</Token-0><RequestId>80294CF916204419</RequestId><HostId>w2MgHVP1/mWBjqz0sNY//b/hNRf5e5TV54NgEwrSFk3z6z54YSwEzC/GGzmjxGcfV/Xs54UskdU=</HostId></Error>`))),
	}, nil)

	t.graphQLClient.On("GetSignedURL", ctx).Return("some.url?signed", "some.url", nil)
	t.graphQLClient.On("ResetURLCache").Return(nil)

	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Size:        int64(len(content)),
	}
	httpClient := &http.Client{Transport: t.httpTransport}

	storeAssetFn := useS3AssetStorage(t.graphQLClient, httpClient)
	unsignedURL, err := storeAssetFn(ctx, asset, bytes.NewReader(content))
	t.Empty(unsignedURL)
	if t.Error(err) {
		t.True(shouldRetry(err))
		t.Contains(err.Error(), "400 Bad Request")
	}

	t.graphQLClient.AssertCalled(t.T(), "ResetURLCache")
}

func (t *storageTestSuite) TestS3AssetStorageImage() {
	ctx := context.Background()

	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Type:        api.AssetTypeMedia,
		ContentType: "image/jpg",
		Size:        int64(len(content)),
	}
	httpClient := &http.Client{Transport: t.httpTransport}

	storeAssetFn := useS3AssetStorage(t.graphQLClient, httpClient)
	url, err := storeAssetFn(ctx, asset, bytes.NewReader(content))
	if t.NoError(err) {
		t.Equal("", url)
	}
}

func (t *storageTestSuite) TestMinioAssetStorageFn() {
	ctx := context.Background()
	t.httpTransport.On("RoundTrip", mock.AnythingOfType("*http.Request")).Run(func(args mock.Arguments) {
		req := args.Get(0).(*http.Request)
		ioutil.ReadAll(req.Body)
		req.Body.Close()
	}).Return(&http.Response{
		Status:     "OK",
		StatusCode: 200,
		Body:       ioutil.NopCloser(bytes.NewReader([]byte("{}"))),
	}, nil)

	minioStorageConfig := MinioStorageConfig{
		MinioServerURLIPBased: "http://54.174.79.89:9101",
		MediaSegmentBucket:    "portableedge",
		MinioServer:           "http://minio-1:9101",
	}
	content := []byte("1234567890!@#$%^&*()")
	asset := &api.Asset{
		Type:        api.AssetTypeMedia,
		ContentType: "video/mp4",
		Size:        int64(len(content)),
	}
	httpClient := &http.Client{Transport: t.httpTransport}

	storeAssetFn := useMinioAssetStorage(httpClient, "tdo123", minioStorageConfig)
	url, err := storeAssetFn(ctx, asset, bytes.NewReader(content))
	if t.NoError(err) {
		t.Contains(url, "http://54.174.79.89:9101/portableedge/assets/")
		t.Equal(".mp4", url[len(url)-4:])
		if t.httpTransport.AssertNumberOfCalls(t.T(), "RoundTrip", 1) {
			req := t.httpTransport.Calls[0].Arguments.Get(0).(*http.Request)
			t.Equal(asset.Size, req.ContentLength)
			t.Contains(req.URL.Path, "/portableedge/assets/")
			t.Equal(".mp4", req.URL.Path[len(req.URL.Path)-4:])
		}
	}

}
