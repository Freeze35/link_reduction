package initprometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"os"
	"time"
)

type PrometheusMetrics struct {
	CreateShortLinkTotal *prometheus.CounterVec
	RedirectTotal        *prometheus.CounterVec
}

func InitPrometheus() *PrometheusMetrics {
	promHost := os.Getenv("PROMETHEUS_HOST")
	if promHost == "" {
		promHost = "http://prometheus:9090" // default for docker-compose
	}

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(promHost + "/-/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}

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
