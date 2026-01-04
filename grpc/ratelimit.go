package server

import (
	"context"
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rigoiot/pkg/logger"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ============================================================
// Metrics
// ============================================================

var grpcRateLimitedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "grpc_rate_limited_total",
		Help: "Rate limited gRPC requests or messages",
	},
	[]string{"method", "direction"},
)

// ============================================================
// Rate Limiter Config
// ============================================================

type RateLimiterConfig struct {
	Rate  float64
	Burst int
}

var DefaultRateLimiterConfig = RateLimiterConfig{
	Rate:  100,
	Burst: 200,
}

var limiterMap sync.Map // map[string]*rate.Limiter

func getLimiter(key string) *rate.Limiter {
	limiter, _ := limiterMap.LoadOrStore(
		key,
		rate.NewLimiter(
			rate.Limit(DefaultRateLimiterConfig.Rate),
			DefaultRateLimiterConfig.Burst,
		),
	)
	return limiter.(*rate.Limiter)
}

// ============================================================
// Client IP
// ============================================================

func getClientIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}

	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// ============================================================
// Unary Interceptor
// ============================================================

func UnaryRateLimitInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		method := info.FullMethod

		if method == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}

		ip := getClientIP(ctx)
		key := ip + "|" + method

		logger.Info(
			"[RATE_LIMIT][INFO] ip=%s method=%s",
			ip, method,
		)

		if !getLimiter(key).Allow() {
			logger.Errorf(
				"[RATE_LIMIT][UNARY] ip=%s method=%s",
				ip, method,
			)

			grpcRateLimitedTotal.WithLabelValues(key).Inc()
			return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded")
		}

		return handler(ctx, req)
	}
}

// ============================================================
// Stream Wrapper
// ============================================================

type rateLimitServerStream struct {
	grpc.ServerStream
	method string
	ip     string
}

func (s *rateLimitServerStream) RecvMsg(m interface{}) error {
	key := s.ip + "|" + s.method + "|recv"

	if !getLimiter(key).Allow() {
		logger.Errorf(
			"[RATE_LIMIT][STREAM_RECV] ip=%s method=%s",
			s.ip, s.method,
		)

		grpcRateLimitedTotal.WithLabelValues(key).Inc()
		return status.Errorf(codes.ResourceExhausted, "stream recv rate limit exceeded")
	}

	return s.ServerStream.RecvMsg(m)
}

func (s *rateLimitServerStream) SendMsg(m interface{}) error {
	key := s.ip + "|" + s.method + "|send"

	if !getLimiter(key).Allow() {
		logger.Errorf(
			"[RATE_LIMIT][STREAM_SEND] ip=%s method=%s",
			s.ip, s.method,
		)

		grpcRateLimitedTotal.WithLabelValues(key).Inc()
		return status.Errorf(codes.ResourceExhausted, "stream send rate limit exceeded")
	}

	return s.ServerStream.SendMsg(m)
}

// ============================================================
// Stream Interceptor
// ============================================================

func StreamRateLimitInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {

		method := info.FullMethod

		if method == "/grpc.health.v1.Health/Watch" {
			return handler(srv, ss)
		}

		ip := getClientIP(ss.Context())

		streamKey := ip + "|" + method + "|stream"
		if !getLimiter(streamKey).Allow() {
			logger.Errorf(
				"[RATE_LIMIT][STREAM_CONN] ip=%s method=%s",
				ip, method,
			)

			grpcRateLimitedTotal.WithLabelValues(streamKey).Inc()
			return status.Errorf(codes.ResourceExhausted, "stream rate limit exceeded")
		}

		wrapped := &rateLimitServerStream{
			ServerStream: ss,
			method:       method,
			ip:           ip,
		}

		return handler(srv, wrapped)
	}
}
