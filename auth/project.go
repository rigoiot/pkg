package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	uuid "github.com/satori/go.uuid"
)

var (
	errMissingProjectID = errors.New("unable to get project id from context")

	// ProjectKey is the metadata key used to carry project identity.
	ProjectKey = "ProjectID"
)

// GetProjectID gets the projectID from a context.
func GetProjectID(ctx context.Context, _ interface{}) (uuid.UUID, error) {
	val := metautils.ExtractIncoming(ctx).Get(ProjectKey)
	if val == "" {
		return uuid.Nil, errMissingProjectID
	}
	id, err := uuid.FromString(val)
	if err != nil {
		return uuid.Nil, errMissingProjectID
	}
	return id, nil
}
