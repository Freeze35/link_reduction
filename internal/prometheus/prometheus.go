package initprometheus

import "github.com/prometheus/client_golang/prometheus"

// PrometheusMetrics - структура для хранения метрик Prometheus
type PrometheusMetrics struct {
	CreateShortLinkTotal *prometheus.CounterVec
	RedirectTotal        *prometheus.CounterVec
}

func InitPrometheus() *PrometheusMetrics {
	metrics := &PrometheusMetrics{
		CreateShortLinkTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_create_short_link_total",
				Help: "Total number of short link creation requests",
			},
			[]string{"status", "reason"},
		),
		RedirectTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "shortener_redirect_total",
				Help: "Total number of redirect requests",
			},
			[]string{"status", "reason"},
		),
	}

	// Регистрация метрик
	prometheus.MustRegister(metrics.CreateShortLinkTotal)
	prometheus.MustRegister(metrics.RedirectTotal)

	return metrics
}
