package streamio

import (
	"context"
	"fmt"
	"io"
	"time"

	awsV1 "github.com/aws/aws-sdk-go/aws"
	sessionV1 "github.com/aws/aws-sdk-go/aws/session"
	s3V1 "github.com/aws/aws-sdk-go/service/s3"
	s3managerV1 "github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
)

const defaultMinioURLTimeout = 6 * time.Hour

type Cacher interface {
	Cache(ctx context.Context, r io.Reader, dest string, mimeType string) (string, error)
}

type S3CacheConfig struct {
	Region      string `json:"region,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	MinioServer string `json:"minioServer,omitempty"`
}

type s3ChunkCache struct {
	bucket   string
	uploader *s3manager.Uploader
}

func NewS3ChunkCache(config S3CacheConfig) (Cacher, error) {
	// running on-prem mode
	if config.MinioServer != "" {
		s3Config := awsV1.Config{
			Endpoint:         awsV1.String(config.MinioServer),
			Region:           awsV1.String(config.Region),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
		}

		session := sessionV1.New(&s3Config)
		uploader := s3managerV1.NewUploader(session)
		s3Service := s3V1.New(session)

		// return a chunk cache object using AWS Go SDK version 1.0, since Minio server does not work with version 2.0
		return &minioChunkCache{
			bucket:   config.Bucket,
			uploader: uploader,
			s3Svc:    s3Service,
		}, nil
	}

	awsCfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, fmt.Errorf("aws configuration err: %s", err)
	}

	awsCfg.Region = config.Region

	return &s3ChunkCache{
		bucket:   config.Bucket,
		uploader: s3manager.NewUploaderWithClient(s3.New(awsCfg)),
	}, nil
}

func (c *s3ChunkCache) Cache(ctx context.Context, r io.Reader, dest string, mimeType string) (string, error) {
	result, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(dest),
		ContentType: aws.String(mimeType),
		ACL:         s3.ObjectCannedACLPublicRead,
		Body:        r,
	})

	if err != nil {
		return "", err
	}

	return result.Location, nil
}

type minioChunkCache struct {
	bucket   string
	s3Svc    *s3V1.S3
	uploader *s3managerV1.Uploader
}

func (c *minioChunkCache) Cache(ctx context.Context, r io.Reader, dest string, mimeType string) (string, error) {
	_, err := c.uploader.UploadWithContext(ctx, &s3managerV1.UploadInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(dest),
		ContentType: awsV1.String(mimeType),
		ACL:         aws.String(s3V1.ObjectCannedACLPublicRead),
		Body:        r,
	}, func(u *s3managerV1.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // 5MB part size
		u.LeavePartsOnError = false  // delete the parts if the upload fails.
	})

	if err != nil {
		return "", err
	}

	req, _ := c.s3Svc.GetObjectRequest(&s3V1.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(dest),
	})
	// need to return a signed url
	signedURL, err := req.Presign(defaultMinioURLTimeout)
	if err != nil {
		return "", err
	}

	return signedURL, nil
}

type streamCacheWriter struct {
	cache        Cacher
	destPath     string
	onFinish     func(string)
	bytesWritten *Progress
}

func NewStreamCacheWriter(cache Cacher, destPath string, onFinish func(string)) StreamWriter {
	return &streamCacheWriter{
		cache:        cache,
		destPath:     destPath,
		onFinish:     onFinish,
		bytesWritten: new(Progress),
	}
}

func (s *streamCacheWriter) BytesWritten() int64 {
	return s.bytesWritten.Count()
}

func (s *streamCacheWriter) WriteStream(ctx context.Context, stream *Stream) error {
	tr := io.TeeReader(stream, s.bytesWritten)
	loc, err := s.cache.Cache(ctx, tr, s.destPath, stream.MimeType)
	if err != nil {
		return err
	}

	if s.onFinish != nil {
		s.onFinish(loc)
	}
	return nil
}
