package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
)

var (
	errMissingAppCode = errors.New("unable to get app code from context")

	// AppCodeKey ...
	AppCodeKey = "AppCode"
)

// GetAppCode gets the appCode from a context
func GetAppCode(ctx context.Context, _ interface{}) (string, error) {
	val := metautils.ExtractIncoming(ctx).Get(AppCodeKey)
	if val == "" {
		return "", errMissingAppCode
	}
	return val, nil
}
