package errors

import (
	"context"
	"regexp"

	"github.com/lib/pq"

	"google.golang.org/grpc/codes"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jinzhu/gorm"
	"github.com/rigoiot/atlas-app-toolkit/errors"
	"github.com/rigoiot/atlas-app-toolkit/errors/mappers/validationerrors"
	"github.com/rigoiot/atlas-app-toolkit/requestid"
	"github.com/sirupsen/logrus"
)

// ErrorMappings ...
var ErrorMappings = []errors.MapFunc{

	// Default Validation Mapping
	validationerrors.DefaultMapping(),

	errors.NewMapping(
		errors.CondEq("NOT_EXISTS"),
		errors.NewContainer(
			codes.InvalidArgument, "The resource does not exist.",
		).WithField(
			"path/id", "the specified object was not found."),
	),

	errors.NewMapping(
		errors.CondHasPrefix("pq:"),
		errors.MapFunc(func(ctx context.Context, err error) (error, bool) {
			if res := regexp.MustCompile(`column "(\w+)" does not exist`).FindStringSubmatch(err.Error()); len(res) > 0 {
				return errors.NewContainer(codes.InvalidArgument, "Invalid collection operator parameter %q.", res[1]), true
			}
			if pqErr, ok := err.(*pq.Error); ok && string(pqErr.Code) == "23505" {
				res := regexp.MustCompile(`\((.*?)\)`).FindAllStringSubmatch(pqErr.Detail, -1)
				if len(res) > 1 {
					return errors.NewContainer(codes.AlreadyExists, "There is already an existing '%s' object with the same '%s'.", res[0][1], res[1][1]).WithField(
						res[0][1], "already exists"), true
				}
			}

			return nil, false
		}),
	),

	errors.NewMapping(
		gorm.ErrRecordNotFound,
		errors.NewContainer(codes.NotFound, "record not found"),
	),

	errors.NewMapping(
		// Here CondAnd without condition functions serves as 'default'.
		errors.CondAnd(),
		errors.MapFunc(func(ctx context.Context, err error) (error, bool) {
			ctxlogrus.AddFields(ctx, logrus.Fields{"internal-error": err})
			reqID, exist := requestid.FromContext(ctx)
			if exist {
				return errors.NewContainer(codes.Internal, "Internal error occured. For more details see log for request %s", reqID), true
			}
			return errors.NewContainer(codes.Internal, "Internal error occured."), true
		}),
	),
}
