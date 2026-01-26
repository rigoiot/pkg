package server

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rigoiot/pkg/logger"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

//
// ============================================================
// Prometheus Metrics
// ============================================================
//

// grpcRateLimitedTotal
// 统计被限流的请求 / 消息数量
// label:
//   - key: ip|method
//   - direction: unary / stream_conn / stream_recv / stream_send
var grpcRateLimitedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "grpc_rate_limited_total",
		Help: "Rate limited gRPC requests or messages",
	},
	[]string{"key", "direction"},
)

//
// ============================================================
// NATS Interface
// ============================================================
//

// NatsPublisher 定义 NATS 发布接口，解耦 NATS 具体实现
type NatsPublisher interface {
	Publish(subject string, data []byte) error
}

//
// ============================================================
// Rate Limiter Config
// ============================================================
//

// RateLimiterConfig
// 所有限流相关配置集中在这里，方便未来做动态配置（Consul / etcd）
type RateLimiterConfig struct {
	Rate             float64 // QPS：每秒允许通过的请求数
	Burst            int     // 突发容量：允许短时间内瞬间放行的请求数
	Concurrent       int     // 每个 IP + Method 的最大并发数
	GlobalConcurrent int     // 整个 gRPC Server 的最大并发数（保命）

	NatsConn       NatsPublisher // NATS 发布器实例，由调用方初始化和管理
	NatsTopic      string        // NATS 限流通知主题
	BypassPatterns []string      // 不进行限流的 gRPC 方法
}

// 默认配置（生产可直接用，偏保守）
var DefaultRateLimiterConfig = RateLimiterConfig{
	Rate:             50,
	Burst:            100,
	Concurrent:       30,
	GlobalConcurrent: 300,

	NatsConn:  nil,
	NatsTopic: "gocloud.rate_limit.alert",
	BypassPatterns: []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch",
	},
}

//
// ============================================================
// Limiter Storage（QPS + 并发）
// ============================================================
//

// limiterBundle
// 每一个 key（ip|method）对应一套完整的限流器
//   - qps  : 令牌桶（限制速率）
//   - conc : 信号量（限制并发）
type limiterBundle struct {
	qps  *rate.Limiter
	conc chan struct{}
}

// limiterMap
// key = ip|method
// value = *limiterBundle
var limiterMap sync.Map

// getLimiter
// 获取或创建某个 key 对应的 limiter
func getLimiter(key string) *limiterBundle {
	v, _ := limiterMap.LoadOrStore(key, &limiterBundle{
		qps: rate.NewLimiter(
			rate.Limit(DefaultRateLimiterConfig.Rate),
			DefaultRateLimiterConfig.Burst,
		),
		conc: make(chan struct{}, DefaultRateLimiterConfig.Concurrent),
	})
	return v.(*limiterBundle)
}

//
// ============================================================
// Config Initialization（配置初始化）
// ============================================================
//

// InitRateLimiterConfig 统一配置限流器参数
// 参数说明：
//   - config: 限流器配置结构体
//   - Rate: 每秒允许的请求数（QPS），必须 > 0，无效时使用默认值
//   - Burst: 突发流量大小，必须 > 0，无效时使用默认值
//   - Concurrent: 每个 IP+Method 的最大并发数，必须 > 0，无效时使用默认值
//   - GlobalConcurrent: 全局最大并发数，必须 > 0，无效时使用默认值
//   - NatsConn: NATS 连接实例，由调用方初始化，为 nil 则不发送通知
//   - NatsTopic: NATS 限流通知主题
//   - BypassPatterns: 跳过限流的路径模式列表，支持精确匹配和前缀匹配（以 * 结尾）
func InitRateLimiterConfig(config RateLimiterConfig) {
	// 验证和设置 rate
	if config.Rate > 0 {
		DefaultRateLimiterConfig.Rate = config.Rate
	}

	// 验证和设置 burst
	if config.Burst > 0 {
		DefaultRateLimiterConfig.Burst = config.Burst
	}

	// 验证和设置 concurrent
	if config.Concurrent > 0 {
		DefaultRateLimiterConfig.Concurrent = config.Concurrent
	}

	// 验证和设置 globalConcurrent
	if config.GlobalConcurrent > 0 {
		DefaultRateLimiterConfig.GlobalConcurrent = config.GlobalConcurrent
		// 重新创建全局信号量
		globalSem = make(chan struct{}, config.GlobalConcurrent)
	}

	// 设置 NATS 配置
	DefaultRateLimiterConfig.NatsConn = config.NatsConn
	if config.NatsTopic != "" {
		DefaultRateLimiterConfig.NatsTopic = config.NatsTopic
	}

	// 设置旁路模式
	if len(config.BypassPatterns) > 0 {
		DefaultRateLimiterConfig.BypassPatterns = config.BypassPatterns
	}

	// 清空现有的限流器，使新配置生效
	limiterMap = sync.Map{}

	natsStatus := "disabled"
	if DefaultRateLimiterConfig.NatsConn != nil {
		natsStatus = "enabled"
	}

	logger.Infof(
		"[RATE_LIMIT][CONFIG] Initialized: rate=%.2f, burst=%d, concurrent=%d, global=%d, nats=%s, topic=%s, bypass=%v",
		DefaultRateLimiterConfig.Rate,
		DefaultRateLimiterConfig.Burst,
		DefaultRateLimiterConfig.Concurrent,
		DefaultRateLimiterConfig.GlobalConcurrent,
		natsStatus,
		DefaultRateLimiterConfig.NatsTopic,
		DefaultRateLimiterConfig.BypassPatterns,
	)
}

//
// ============================================================
// Global Concurrency Limiter（全局保命）
// ============================================================
//

// globalSem
// 控制整个 gRPC Server 同时在处理的请求数
// 防止：
//   - goroutine 无限增长
//   - DB / 下游 RPC 被拖死
var globalSem = make(chan struct{}, DefaultRateLimiterConfig.GlobalConcurrent)

// acquireGlobal
// 尝试获取一个全局并发名额
func acquireGlobal() bool {
	select {
	case globalSem <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseGlobal
// 释放全局并发名额
func releaseGlobal() {
	<-globalSem
}

//
// ============================================================
// Client IP
// ============================================================
//

// getClientIP
// 从 gRPC context 中解析客户端 IP
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

//
// ============================================================
// Bypass Logic（跳过限流）
// ============================================================
//

// isBypassMethod
// 判断某个 gRPC 方法是否需要跳过限流
// 支持：
//   - 精确匹配
//   - 前缀匹配（以 * 结尾）
func isBypassMethod(method string) bool {
	for _, pattern := range DefaultRateLimiterConfig.BypassPatterns {
		if pattern == method {
			return true
		}
		if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
			prefix := pattern[:len(pattern)-1]
			if len(method) >= len(prefix) && method[:len(prefix)] == prefix {
				return true
			}
		}
	}
	return false
}

//
// ============================================================
// NATS Notification（异步报警）
// ============================================================
//

// RateLimitEvent
// NATS 发送的数据结构
type RateLimitEvent struct {
	Timestamp string `json:"timestamp"`
	IP        string `json:"ip"`
	Method    string `json:"method"`
	Direction string `json:"direction"`
	Message   string `json:"message"`
}

// sendNatsNotification
// 异步发送 NATS 消息，避免阻塞主流程
func sendNatsNotification(ip, method, direction string) {
	// 如果未配置 NATS 连接，直接返回
	if DefaultRateLimiterConfig.NatsConn == nil {
		return
	}

	go func() {
		event := RateLimitEvent{
			Timestamp: time.Now().Format(time.RFC3339),
			IP:        ip,
			Method:    method,
			Direction: direction,
			Message:   "Rate limit exceeded",
		}

		payload, err := json.Marshal(event)
		if err != nil {
			logger.Errorf("[RATE_LIMIT][NATS] marshal error: %v", err)
			return
		}

		err = DefaultRateLimiterConfig.NatsConn.Publish(DefaultRateLimiterConfig.NatsTopic, payload)
		if err != nil {
			logger.Errorf("[RATE_LIMIT][NATS] publish error: %v", err)
		}
	}()
}

//
// ============================================================
// Unary Interceptor（普通 RPC）
// ============================================================
//

func UnaryRateLimitInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		method := info.FullMethod

		// 健康检查等接口直接放行
		if isBypassMethod(method) {
			return handler(ctx, req)
		}

		// 提前获取 IP，避免重复调用
		ip := getClientIP(ctx)

		// ① 全局并发限制（最先做，防止 goroutine 堆积）
		if !acquireGlobal() {
			grpcRateLimitedTotal.WithLabelValues("global", "unary").Inc()
			sendNatsNotification(ip, method, "unary_global_concurrent")
			return nil, status.Error(codes.Internal, "server busy")
		}
		defer releaseGlobal()

		key := ip + "|" + method
		limiter := getLimiter(key)

		// ② QPS 限流（削峰）
		if !limiter.qps.Allow() {
			grpcRateLimitedTotal.WithLabelValues(key, "unary").Inc()
			sendNatsNotification(ip, method, "unary_qps")
			return nil, status.Error(codes.Internal, "rate limit exceeded")
		}

		// ③ 并发限制（防慢接口拖垮）
		select {
		case limiter.conc <- struct{}{}:
			defer func() { <-limiter.conc }()
		default:
			grpcRateLimitedTotal.WithLabelValues(key, "unary").Inc()
			sendNatsNotification(ip, method, "unary_concurrent")
			return nil, status.Error(codes.Internal, "too many concurrent requests")
		}

		return handler(ctx, req)
	}
}

//
// ============================================================
// Stream Wrapper（消息级限流）
// ============================================================
//

// rateLimitServerStream
// 包装 ServerStream，实现 RecvMsg / SendMsg 拦截
type rateLimitServerStream struct {
	grpc.ServerStream
	method  string
	ip      string
	limiter *limiterBundle
}

// RecvMsg
// 对 stream 的每一条接收消息做限流
func (s *rateLimitServerStream) RecvMsg(m interface{}) error {
	if !s.limiter.qps.Allow() {
		grpcRateLimitedTotal.WithLabelValues(
			s.ip+"|"+s.method,
			"stream_recv",
		).Inc()
		sendNatsNotification(s.ip, s.method, "stream_recv_qps")
		return status.Error(codes.Internal, "stream recv rate limit exceeded")
	}
	return s.ServerStream.RecvMsg(m)
}

// SendMsg
// 对 stream 的每一条发送消息做限流
func (s *rateLimitServerStream) SendMsg(m interface{}) error {
	if !s.limiter.qps.Allow() {
		grpcRateLimitedTotal.WithLabelValues(
			s.ip+"|"+s.method,
			"stream_send",
		).Inc()
		sendNatsNotification(s.ip, s.method, "stream_send_qps")
		return status.Error(codes.Internal, "stream send rate limit exceeded")
	}
	return s.ServerStream.SendMsg(m)
}

//
// ============================================================
// Stream Interceptor（连接级 + 并发）
// ============================================================
//

func StreamRateLimitInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {

		method := info.FullMethod

		if isBypassMethod(method) {
			return handler(srv, ss)
		}

		// 提前获取 IP，避免重复调用
		ip := getClientIP(ss.Context())

		// ① 全局并发限制
		if !acquireGlobal() {
			grpcRateLimitedTotal.WithLabelValues("global", "stream").Inc()
			sendNatsNotification(ip, method, "stream_global_concurrent")
			return status.Error(codes.Internal, "server busy")
		}
		defer releaseGlobal()

		key := ip + "|" + method
		limiter := getLimiter(key)

		// ② stream 级并发限制
		select {
		case limiter.conc <- struct{}{}:
			defer func() { <-limiter.conc }()
		default:
			grpcRateLimitedTotal.WithLabelValues(key, "stream").Inc()
			sendNatsNotification(ip, method, "stream_concurrent")
			return status.Error(codes.Internal, "too many concurrent streams")
		}

		// ③ 包装 stream，实现消息级限流
		wrapped := &rateLimitServerStream{
			ServerStream: ss,
			method:       method,
			ip:           ip,
			limiter:      limiter,
		}

		return handler(srv, wrapped)
	}
}
