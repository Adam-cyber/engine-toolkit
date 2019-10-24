package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/machinebox/graphql"
	"github.com/pkg/errors"
	"github.com/veritone/engine-toolkit/engine/internal/vericlient"
)

// Asset represents a file.
type Asset struct {
	ID               string
	Name             string
	SignedURI        string
	ContentType      string
	AssetType        string
	CreatedDateTime  string
	ModifiedDateTime string
	FileData         FileData
	JSONData         map[string]interface{}
}

// FileData describes information about a file.
type FileData struct {
	MD5Sum string `json:"md5sum"`
	Size   int    `json:"size"`
}

// SourceData describes the source of an asset (which engine/task created it).
type SourceData struct {
	Name   string `json:"name"`
	TaskID string `json:"taskId"`
}

// AssetCreate holds information about a new Asset.
type AssetCreate struct {
	ContainerTDOID string                 `json:"containerId"`
	Name           string                 `json:"name"`
	Description    string                 `json:"description"`
	ContentType    string                 `json:"contentType"`
	AssetType      string                 `json:"assetType,omitempty"`
	Details        map[string]interface{} `json:"details,omitempty"`
	FileData       *FileData              `json:"fileData,omitempty"`
	SourceData     *SourceData            `json:"sourceData,omitempty"`
	Body           io.Reader              `json:"-"`
}

// Do creates the asset.
func (c *AssetCreate) Do(ctx context.Context, client vericlient.GraphQLClient) (*Asset, error) {
	req := graphql.NewRequest(`
mutation CreateAsset(	$containerId: ID!, $name: String, 
						$description: String,  $contentType: String, $assetType: String,
						$md5sum: String, $size: Int, $details: JSONData,
						$sourceName: String, $sourceTaskId: ID) {
	createAsset (
		input: {
			containerId: $containerId,
			name: $name,
			description: $description,
			contentType: $contentType,
			assetType: $assetType,
			details: $details,
			fileData: {
			md5sum: $md5sum,
			size: $size
		},
		sourceData: {
			name: $sourceName,
			taskId: $sourceTaskId
		}
	}) {
		id
		signedUri
		contentType
		createdDateTime
		modifiedDateTime
		jsondata
		assetType
		fileData {
			md5sum
			size
		}
	}
}
`)
	b, err := json.Marshal(c)
	if err != nil {
		return nil, errors.Wrap(err, "json encode CreateAsset")
	}
	req.Var("containerId", c.ContainerTDOID)
	req.Var("name", c.Name)
	req.Var("description", c.Description)
	req.Var("contentType", c.ContentType)
	req.Var("assetType", c.AssetType)
	if c.FileData != nil {
		req.Var("md5sum", c.FileData.MD5Sum)
		req.Var("size", c.FileData.Size)
	}
	if c.SourceData != nil {
		req.Var("sourceName", c.SourceData.Name)
		req.Var("sourceTaskId", c.SourceData.TaskID)
	}
	b, err = json.Marshal(c.Details)
	if err != nil {
		return nil, errors.Wrap(err, "encoding details")
	}
	req.Var("details", string(b))
	req.File("file", c.Name, c.Body)
	var response struct {
		CreateAsset Asset
	}
	if err := client.Run(ctx, req, &response); err != nil {
		return nil, err
	}
	return &response.CreateAsset, nil
}
