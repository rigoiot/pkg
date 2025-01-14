package auth

import (
	"context"

	"google.golang.org/grpc/metadata"

	"google.golang.org/grpc"
)

// UnaryServerInterceptor returns a new unary server interceptor that inject grpc client.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// put account id in metadata
		md := metadata.MD{}
		accountID, err := GetAccountID(ctx, nil)
		if err == nil {
			md.Append(multiAccountKey, accountID.String())
		}
		userID, err := GetUserID(ctx, nil)
		if err == nil {
			md.Append(UserKey, userID.String())
		}
		appCode, err := GetAppCode(ctx, nil)
		if err == nil {
			md.Append(AppCodeKey, appCode)
		}
		ctx = metadata.NewOutgoingContext(ctx, md)
		return handler(ctx, req)
	}
}
