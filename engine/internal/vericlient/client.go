package vericlient

import (
	"context"
	"net/http"
	"time"

	"github.com/machinebox/graphql"
	"github.com/veritone/engine-toolkit/engine/internal/backoff"
)

// GraphQLClient is a graphQL client.
type GraphQLClient interface {
	Run(ctx context.Context, req *graphql.Request, resp interface{}) error
}

// GraphQLClientFunc is a function wrapper for GraphQLClient.
type GraphQLClientFunc func(ctx context.Context, req *graphql.Request, resp interface{}) error

// Run execites the request.
func (c GraphQLClientFunc) Run(ctx context.Context, req *graphql.Request, resp interface{}) error {
	return c(ctx, req, resp)
}

// WithBackoff wraps a GraphQLClient and implements default backoff behaviour.
func WithBackoff(client GraphQLClient) GraphQLClient {
	backoff := backoff.NewDoubleTimeBackoff(100*time.Millisecond, 7*time.Second, 5)
	return GraphQLClientFunc(func(ctx context.Context, req *graphql.Request, resp interface{}) error {
		return backoff.Do(ctx, func() error {
			return client.Run(ctx, req, resp)
		})
	})
}

// NewClient makes a new graphql.Client capable of speaking to the Veritone API
// at the specified endpoint.
// Specify an empty token for a client that makes non-authenticated requests.
func NewClient(httpclient *http.Client, token, endpoint string) GraphQLClient {
	if httpclient == nil {
		httpclient = http.DefaultClient
	}
	rt := httpclient.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	if token != "" {
		authorizationHeader := "Bearer " + token
		httpclient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			req.Header.Set("Authorization", authorizationHeader)
			req.Header.Set("X-Client", "github.com/veritone/engine-toolkit/engine/internal/vericlient")
			return rt.RoundTrip(req)
		})
	}
	client := graphql.NewClient(endpoint, graphql.WithHTTPClient(httpclient), graphql.UseMultipartForm())
	return WithBackoff(client)
}

// NewClientLog makes a new graphql.Client capable of speaking to the Veritone API
// at the specified endpoint.
// Specify an empty token for a client that makes non-authenticated requests.
func NewClientLog(httpclient *http.Client, log func(s string), token, endpoint string) GraphQLClient {
	if httpclient == nil {
		httpclient = http.DefaultClient
	}
	rt := httpclient.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	if token != "" {
		authorizationHeader := "Bearer " + token
		httpclient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			req.Header.Set("Authorization", authorizationHeader)
			req.Header.Set("X-Client", "github.com/veritone/machinebox-engines/engines/iron/helpers/veritoneclient")
			return rt.RoundTrip(req)
		})
	}
	client := graphql.NewClient(endpoint, graphql.WithHTTPClient(httpclient), graphql.UseMultipartForm())
	client.Log = log
	return WithBackoff(client)
}

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}
