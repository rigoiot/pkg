package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	uuid "github.com/satori/go.uuid"
)

var (
	errMissingUserID = errors.New("unable to get user id from context")

	// UserKey ...
	UserKey = "UserID"
)

// GetUserID gets the account from a context
func GetUserID(ctx context.Context, _ interface{}) (uuid.UUID, error) {
	val := metautils.ExtractIncoming(ctx).Get(UserKey)
	if val == "" {
		return uuid.Nil, errMissingUserID
	}
	id, err := uuid.FromString(val)
	if err != nil {
		return uuid.Nil, errMissingUserID
	}
	return id, nil
}
