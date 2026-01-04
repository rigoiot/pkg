package server

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rigoiot/pkg/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ============================================================
// Prometheus Metrics
// ============================================================

var (
	grpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of gRPC requests",
		},
		[]string{"method", "code"},
	)

	grpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_duration_seconds",
			Help:    "gRPC request duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	grpcActiveRequests = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "grpc_active_requests",
			Help: "Active gRPC requests",
		},
		[]string{"method"},
	)

	grpcRequestSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_request_size_bytes",
			Help:    "Request size",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		},
		[]string{"method"},
	)

	grpcResponseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_response_size_bytes",
			Help:    "Response size",
			Buckets: prometheus.ExponentialBuckets(100, 10, 7),
		},
		[]string{"method"},
	)
)

// ============================================================
// Metrics Interceptors
// ============================================================

func UnaryMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {

		method := info.FullMethod

		grpcActiveRequests.WithLabelValues(method).Inc()
		defer grpcActiveRequests.WithLabelValues(method).Dec()

		timer := prometheus.NewTimer(
			grpcRequestDuration.WithLabelValues(method),
		)
		defer timer.ObserveDuration()

		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("panic in unary %s: %v", method, r)
				err = status.Errorf(codes.Internal, "internal server error")
			}

			code := codes.OK
			if err != nil {
				if s, ok := status.FromError(err); ok {
					code = s.Code()
				} else {
					code = codes.Unknown
				}
			}
			grpcRequestsTotal.WithLabelValues(method, code.String()).Inc()
		}()

		if msg, ok := req.(proto.Message); ok {
			grpcRequestSize.WithLabelValues(method).
				Observe(float64(proto.Size(msg)))
		}

		resp, err = handler(ctx, req)

		if msg, ok := resp.(proto.Message); ok {
			grpcResponseSize.WithLabelValues(method).
				Observe(float64(proto.Size(msg)))
		}

		return resp, err
	}
}

func StreamMetricsInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {

		method := info.FullMethod

		grpcActiveRequests.WithLabelValues(method).Inc()
		defer grpcActiveRequests.WithLabelValues(method).Dec()

		timer := prometheus.NewTimer(
			grpcRequestDuration.WithLabelValues(method),
		)
		defer timer.ObserveDuration()

		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("panic in stream %s: %v", method, r)
				err = status.Errorf(codes.Internal, "internal server error")
			}

			code := codes.OK
			if err != nil {
				if s, ok := status.FromError(err); ok {
					code = s.Code()
				} else {
					code = codes.Unknown
				}
			}
			grpcRequestsTotal.WithLabelValues(method, code.String()).Inc()
		}()

		return handler(srv, ss)
	}
}
