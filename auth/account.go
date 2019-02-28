package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
)

var (
	errMissingAccountID = errors.New("unable to get account id from context")

	// multiAccountKey
	multiAccountKey = "AccountID"
)

// GetAccountID gets the account from a context
func GetAccountID(ctx context.Context, _ interface{}) (string, error) {
	val := metautils.ExtractIncoming(ctx).Get(multiAccountKey)
	if val == "" {
		return "", errMissingAccountID
	}
	return val, nil
}
