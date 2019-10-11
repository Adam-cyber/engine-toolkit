package api

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestAPIPackage(t *testing.T) {
	suite.Run(t, new(graphqlTestSuite))
}
