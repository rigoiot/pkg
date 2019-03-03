package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	uuid "github.com/satori/go.uuid"
)

var (
	errMissingAccountID = errors.New("unable to get account id from context")

	// multiAccountKey
	multiAccountKey = "AccountID"
)

// GetAccountID gets the account from a context
func GetAccountID(ctx context.Context, _ interface{}) (uuid.UUID, error) {
	val := metautils.ExtractIncoming(ctx).Get(multiAccountKey)
	if val == "" {
		return uuid.Nil, errMissingAccountID
	}
	id, err := uuid.FromString(val)
	if err != nil {
		return uuid.Nil, errMissingAccountID
	}
	return id, nil
}
