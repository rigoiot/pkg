package auth

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
)

var (
	errMissingLocale = errors.New("unable to get locale from context")

	// LocaleKey ...
	LocaleKey = "Locale"
)

// GetLocale gets the locale from a context
func GetLocale(ctx context.Context, _ interface{}) (string, error) {
	val := metautils.ExtractIncoming(ctx).Get(LocaleKey)
	if val == "" {
		return "", errMissingLocale
	}
	return val, nil
}
