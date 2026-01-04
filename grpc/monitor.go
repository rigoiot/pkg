package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/rigoiot/pkg/logger"
)

// MonitorServer HTTP监控服务器
type MonitorServer struct {
	port int
}

// NewMonitorServer 创建监控服务器
func NewMonitorServer(port int) *MonitorServer {
	return &MonitorServer{port: port}
}

// Start 启动监控HTTP服务器
func (m *MonitorServer) Start() {
	mux := http.NewServeMux()

	// Prometheus 指标端点
	mux.Handle("/metrics", promhttp.Handler())

	// 服务统计端点
	mux.HandleFunc("/stats", m.statsHandler)

	// 单个服务详情端点
	mux.HandleFunc("/stats/", m.serviceStatsHandler)

	addr := fmt.Sprintf(":%d", m.port)
	logger.Infof("Prometheus metrics HTTP server starting on %s", addr)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Errorf("Prometheus metrics HTTP server error: %v", err)
		}
	}()
}

// StartMonitor 启动监控服务的便捷函数
func StartMonitor(port int) {
	if port <= 0 {
		port = 8080 // 默认端口
	}
	monitor := NewMonitorServer(port)
	monitor.Start()
}

// ========== 服务统计相关 ==========

// ServiceStats 单个服务的统计数据
type ServiceStats struct {
	Service        string             `json:"service"`
	TotalRequests  float64            `json:"total_requests"`
	SuccessCount   float64            `json:"success_count"`
	FailedCount    float64            `json:"failed_count"`
	SuccessRate    float64            `json:"success_rate"`
	RateLimited    float64            `json:"rate_limited"`
	ActiveRequests float64            `json:"active_requests"`
	AvgDurationMs  float64            `json:"avg_duration_ms"`
	StatusCodes    map[string]float64 `json:"status_codes"`
}

// AllServicesStats 所有服务的统计数据
type AllServicesStats struct {
	TotalRequests    float64                  `json:"total_requests"`
	TotalRateLimited float64                  `json:"total_rate_limited"`
	Services         map[string]*ServiceStats `json:"services"`
}

// statsHandler 返回所有服务的统计数据
func (m *MonitorServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	window := r.URL.Query().Get("window")

	var stats *AllServicesStats
	if window == "" {
		stats = m.collectAllStats() // 原来的
	} else {
		stats = m.collectAllStatsByWindow(window)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// serviceStatsHandler 返回单个服务的统计数据
func (m *MonitorServer) serviceStatsHandler(w http.ResponseWriter, r *http.Request) {
	// 从URL中提取服务名
	serviceName := strings.TrimPrefix(r.URL.Path, "/stats/")
	if serviceName == "" {
		http.Error(w, "service name required", http.StatusBadRequest)
		return
	}

	stats := m.collectAllStats()

	// 查找匹配的服务
	var matchedStats []*ServiceStats
	for method, svc := range stats.Services {
		if strings.Contains(method, serviceName) {
			matchedStats = append(matchedStats, svc)
		}
	}

	if len(matchedStats) == 0 {
		http.Error(w, "service not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(matchedStats)
}

// collectAllStats 收集所有服务的统计数据
func (m *MonitorServer) collectAllStats() *AllServicesStats {
	stats := &AllServicesStats{
		Services: make(map[string]*ServiceStats),
	}

	// 收集请求总数指标
	requestMetrics := collectCounterVecMetrics(grpcRequestsTotal)
	for _, md := range requestMetrics {
		method := md.Labels["method"]
		code := md.Labels["code"]
		value := md.Value

		if _, exists := stats.Services[method]; !exists {
			stats.Services[method] = &ServiceStats{
				Service:     method,
				StatusCodes: make(map[string]float64),
			}
		}

		svc := stats.Services[method]
		svc.TotalRequests += value
		svc.StatusCodes[code] = value
		stats.TotalRequests += value

		if code == "OK" {
			svc.SuccessCount += value
		} else {
			svc.FailedCount += value
		}
	}

	// 计算成功率
	for _, svc := range stats.Services {
		if svc.TotalRequests > 0 {
			svc.SuccessRate = svc.SuccessCount / svc.TotalRequests * 100
		}
	}

	// 收集限流指标
	rateLimitMetrics := collectCounterVecMetrics(grpcRateLimitedTotal)
	for _, md := range rateLimitMetrics {
		method := md.Labels["method"]
		value := md.Value
		if svc, exists := stats.Services[method]; exists {
			svc.RateLimited = value
		} else {
			stats.Services[method] = &ServiceStats{
				Service:     method,
				RateLimited: value,
				StatusCodes: make(map[string]float64),
			}
		}
		stats.TotalRateLimited += value
	}

	// 收集活跃请求指标
	activeMetrics := collectGaugeVecMetrics(grpcActiveRequests)
	for _, md := range activeMetrics {
		method := md.Labels["method"]
		if svc, exists := stats.Services[method]; exists {
			svc.ActiveRequests = md.Value
		}
	}

	// 收集平均响应时间
	durationMetrics := collectHistogramVecMetrics(grpcRequestDuration)
	for method, histData := range durationMetrics {
		if svc, exists := stats.Services[method]; exists {
			if histData.count > 0 {
				svc.AvgDurationMs = (histData.sum / histData.count) * 1000 // 转换为毫秒
			}
		}
	}

	return stats
}

// histogramData 直方图数据
type histogramData struct {
	sum   float64
	count float64
}

// MetricData 指标数据
type MetricData struct {
	Labels map[string]string
	Value  float64
}

// collectCounterVecMetrics 收集 CounterVec 指标
func collectCounterVecMetrics(cv *prometheus.CounterVec) []MetricData {
	var result []MetricData

	ch := make(chan prometheus.Metric, 100)
	go func() {
		cv.Collect(ch)
		close(ch)
	}()

	for metric := range ch {
		var m dto.Metric
		if err := metric.Write(&m); err != nil {
			continue
		}

		labels := make(map[string]string)
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}

		if m.Counter != nil {
			result = append(result, MetricData{
				Labels: labels,
				Value:  m.Counter.GetValue(),
			})
		}
	}

	return result
}

// collectGaugeVecMetrics 收集 GaugeVec 指标
func collectGaugeVecMetrics(gv *prometheus.GaugeVec) []MetricData {
	var result []MetricData

	ch := make(chan prometheus.Metric, 100)
	go func() {
		gv.Collect(ch)
		close(ch)
	}()

	for metric := range ch {
		var m dto.Metric
		if err := metric.Write(&m); err != nil {
			continue
		}

		labels := make(map[string]string)
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}

		if m.Gauge != nil {
			result = append(result, MetricData{
				Labels: labels,
				Value:  m.Gauge.GetValue(),
			})
		}
	}

	return result
}

// collectHistogramVecMetrics 收集 HistogramVec 指标
func collectHistogramVecMetrics(hv *prometheus.HistogramVec) map[string]*histogramData {
	result := make(map[string]*histogramData)

	ch := make(chan prometheus.Metric, 100)
	go func() {
		hv.Collect(ch)
		close(ch)
	}()

	for metric := range ch {
		var m dto.Metric
		if err := metric.Write(&m); err != nil {
			continue
		}

		var method string
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "method" {
				method = lp.GetValue()
				break
			}
		}

		if m.Histogram != nil && method != "" {
			result[method] = &histogramData{
				sum:   m.Histogram.GetSampleSum(),
				count: float64(m.Histogram.GetSampleCount()),
			}
		}
	}

	return result
}

type PromClient struct {
	addr string
	http *http.Client
}

func NewPromClient(addr string) *PromClient {
	return &PromClient{
		addr: addr,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *PromClient) Query(ctx context.Context, promql string) (map[string]float64, error) {
	req, _ := http.NewRequestWithContext(
		ctx,
		"GET",
		c.addr+"/api/v1/query?query="+url.QueryEscape(promql),
		nil,
	)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var r struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []interface{}     `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}

	result := make(map[string]float64)
	for _, item := range r.Data.Result {
		method := item.Metric["method"]
		if len(item.Value) == 2 {
			v, _ := strconv.ParseFloat(item.Value[1].(string), 64)
			result[method] = v
		}
	}
	return result, nil
}

func (c *PromClient) MustQuery(ctx context.Context, promql string) map[string]float64 {
	res, err := c.Query(ctx, promql)
	if err != nil {
		logger.Errorf("prom query failed: %s, err=%v", promql, err)
		return map[string]float64{}
	}
	return res
}

func (m *MonitorServer) collectAllStatsByWindow(window string) *AllServicesStats {
	stats := &AllServicesStats{
		Services: make(map[string]*ServiceStats),
	}

	prom := NewPromClient("http://localhost:9090")
	ctx := context.Background()

	// 1. 请求数
	reqs := prom.MustQuery(ctx,
		fmt.Sprintf(`sum(increase(grpc_requests_total[%s])) by (method)`, window),
	)

	// 2. 成功数
	okReqs := prom.MustQuery(ctx,
		fmt.Sprintf(`sum(increase(grpc_requests_total{code="OK"}[%s])) by (method)`, window),
	)

	// 3. 限流
	rateLimits := prom.MustQuery(ctx,
		fmt.Sprintf(`sum(increase(grpc_rate_limited_total[%s])) by (method)`, window),
	)

	// 4. 平均耗时
	durationSum := prom.MustQuery(ctx,
		fmt.Sprintf(`sum(increase(grpc_request_duration_seconds_sum[%s])) by (method)`, window),
	)
	durationCnt := prom.MustQuery(ctx,
		fmt.Sprintf(`sum(increase(grpc_request_duration_seconds_count[%s])) by (method)`, window),
	)

	for method, total := range reqs {
		svc := &ServiceStats{
			Service:       method,
			TotalRequests: total,
			SuccessCount:  okReqs[method],
			FailedCount:   total - okReqs[method],
			RateLimited:   rateLimits[method],
			StatusCodes:   make(map[string]float64),
		}

		if total > 0 {
			svc.SuccessRate = svc.SuccessCount / total * 100
		}

		if durationCnt[method] > 0 {
			svc.AvgDurationMs =
				(durationSum[method] / durationCnt[method]) * 1000
		}

		stats.Services[method] = svc
		stats.TotalRequests += total
		stats.TotalRateLimited += svc.RateLimited
	}

	return stats
}
