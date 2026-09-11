package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string `json:"http_addr"`
	RedisAddr   string `json:"redis_addr"`
	WorkerCount int    `json:"worker_count"`
	MaxRetries  int    `json:"max_retries"`

	VisibilityTimeout    time.Duration `json:"visibility_timeout"`
	ReaperInterval       time.Duration `json:"reaper_interval"`
	DelayedMoverInterval time.Duration `json:"delayed_mover_interval"`
	MetricsInterval      time.Duration `json:"metrics_interval"`

	LogFormat  string `json:"log_format"`
	LogLevel   string `json:"log_level"`
	ConfigFile string `json:"-"`

	Broker       string `json:"broker"` // redis | rabbitmq | kafka
	RabbitURL    string `json:"rabbit_url"`
	KafkaBrokers string `json:"kafka_brokers"`
	KafkaTopic   string `json:"kafka_topic"`
}

type fileConfig struct {
	HTTPAddr             string `json:"http_addr"`
	RedisAddr            string `json:"redis_addr"`
	WorkerCount          *int   `json:"worker_count"`
	MaxRetries           *int   `json:"max_retries"`
	VisibilityTimeout    string `json:"visibility_timeout"`
	ReaperInterval       string `json:"reaper_interval"`
	DelayedMoverInterval string `json:"delayed_mover_interval"`
	MetricsInterval      string `json:"metrics_interval"`
	LogFormat            string `json:"log_format"`
	LogLevel             string `json:"log_level"`
	Broker               string `json:"broker"`
	RabbitURL            string `json:"rabbit_url"`
	KafkaBrokers         string `json:"kafka_brokers"`
	KafkaTopic           string `json:"kafka_topic"`
}

func Load() (*Config, error) {
	cfg := defaults()

	path := firstNonEmpty(os.Getenv("CONFIG_FILE"), "config.json")
	if _, err := os.Stat(path); err == nil {
		if err := loadFile(path, cfg); err != nil {
			return nil, fmt.Errorf("config file %s: %w", path, err)
		}
		cfg.ConfigFile = path
	} else if os.Getenv("CONFIG_FILE") != "" {
		return nil, fmt.Errorf("CONFIG_FILE=%s: %w", path, err)
	}

	if err := applyEnv(cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		HTTPAddr:             ":8080",
		RedisAddr:            "localhost:6379",
		WorkerCount:          1,
		MaxRetries:           3,
		VisibilityTimeout:    30 * time.Second,
		ReaperInterval:       5 * time.Second,
		DelayedMoverInterval: 2 * time.Second,
		MetricsInterval:      2 * time.Second,
		LogFormat:            "text",
		LogLevel:             "info",
		Broker:               "redis",
		RabbitURL:            "amqp://guest:guest@localhost:5672/",
		KafkaBrokers:         "localhost:9092",
		KafkaTopic:           "jobs",
	}
}

func loadFile(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var fc fileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("parse json: %w", err)
	}

	if fc.HTTPAddr != "" {
		cfg.HTTPAddr = fc.HTTPAddr
	}
	if fc.RedisAddr != "" {
		cfg.RedisAddr = fc.RedisAddr
	}
	if fc.WorkerCount != nil {
		cfg.WorkerCount = *fc.WorkerCount
	}
	if fc.MaxRetries != nil {
		cfg.MaxRetries = *fc.MaxRetries
	}
	if fc.VisibilityTimeout != "" {
		d, err := time.ParseDuration(fc.VisibilityTimeout)
		if err != nil {
			return fmt.Errorf("visibility_timeout: %w", err)
		}
		cfg.VisibilityTimeout = d
	}
	if fc.ReaperInterval != "" {
		d, err := time.ParseDuration(fc.ReaperInterval)
		if err != nil {
			return fmt.Errorf("reaper_interval: %w", err)
		}
		cfg.ReaperInterval = d
	}
	if fc.DelayedMoverInterval != "" {
		d, err := time.ParseDuration(fc.DelayedMoverInterval)
		if err != nil {
			return fmt.Errorf("delayed_mover_interval: %w", err)
		}
		cfg.DelayedMoverInterval = d
	}
	if fc.MetricsInterval != "" {
		d, err := time.ParseDuration(fc.MetricsInterval)
		if err != nil {
			return fmt.Errorf("metrics_interval: %w", err)
		}
		cfg.MetricsInterval = d
	}
	if fc.LogFormat != "" {
		cfg.LogFormat = fc.LogFormat
	}
	if fc.LogLevel != "" {
		cfg.LogLevel = fc.LogLevel
	}
	if fc.Broker != "" {
		cfg.Broker = strings.ToLower(strings.TrimSpace(fc.Broker))
	}
	if fc.RabbitURL != "" {
		cfg.RabbitURL = fc.RabbitURL
	}
	if fc.KafkaBrokers != "" {
		cfg.KafkaBrokers = fc.KafkaBrokers
	}
	if fc.KafkaTopic != "" {
		cfg.KafkaTopic = fc.KafkaTopic
	}
	return nil
}

func applyEnv(cfg *Config) error {
	if v := os.Getenv("HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		cfg.RedisAddr = v
	}
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("WORKER_COUNT=%q: must be an integer", v)
		}
		cfg.WorkerCount = n
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("MAX_RETRIES=%q: must be an integer", v)
		}
		cfg.MaxRetries = n
	}
	if v := os.Getenv("VISIBILITY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("VISIBILITY_TIMEOUT=%q: %w", v, err)
		}
		cfg.VisibilityTimeout = d
	}
	if v := os.Getenv("REAPER_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("REAPER_INTERVAL=%q: %w", v, err)
		}
		cfg.ReaperInterval = d
	}
	if v := os.Getenv("DELAYED_MOVER_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("DELAYED_MOVER_INTERVAL=%q: %w", v, err)
		}
		cfg.DelayedMoverInterval = d
	}
	if v := os.Getenv("METRICS_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("METRICS_INTERVAL=%q: %w", v, err)
		}
		cfg.MetricsInterval = d
	}
	if v := os.Getenv("LOG_FORMAT"); v != "" {
		cfg.LogFormat = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("BROKER"); v != "" {
		cfg.Broker = strings.ToLower(strings.TrimSpace(v))
	}
	if v := os.Getenv("RABBIT_URL"); v != "" {
		cfg.RabbitURL = v
	}
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.KafkaBrokers = v
	}
	if v := os.Getenv("KAFKA_TOPIC"); v != "" {
		cfg.KafkaTopic = v
	}
	return nil
}

func (c *Config) Validate() error {
	var errs []string

	if strings.TrimSpace(c.RedisAddr) == "" {
		errs = append(errs, "redis_addr is required")
	}
	if strings.TrimSpace(c.HTTPAddr) == "" {
		errs = append(errs, "http_addr is required")
	}
	if c.WorkerCount <= 0 {
		errs = append(errs, fmt.Sprintf("worker_count must be > 0 (got %d)", c.WorkerCount))
	}
	if c.MaxRetries < 0 {
		errs = append(errs, fmt.Sprintf("max_retries must be >= 0 (got %d)", c.MaxRetries))
	}
	if c.VisibilityTimeout <= 0 {
		errs = append(errs, "visibility_timeout must be > 0")
	}
	if c.ReaperInterval <= 0 {
		errs = append(errs, "reaper_interval must be > 0")
	}
	if c.DelayedMoverInterval <= 0 {
		errs = append(errs, "delayed_mover_interval must be > 0")
	}
	if c.MetricsInterval <= 0 {
		errs = append(errs, "metrics_interval must be > 0")
	}
	if c.VisibilityTimeout < c.ReaperInterval {
		errs = append(errs, "visibility_timeout should be >= reaper_interval")
	}

	broker := strings.ToLower(strings.TrimSpace(c.Broker))
	if broker == "" {
		broker = "redis"
		c.Broker = "redis"
	}
	switch broker {
	case "redis", "rabbitmq", "rabbit", "kafka":
		c.Broker = broker
	default:
		errs = append(errs, fmt.Sprintf("broker must be redis|rabbitmq|kafka (got %q)", c.Broker))
	}
	if broker == "rabbitmq" || broker == "rabbit" {
		if strings.TrimSpace(c.RabbitURL) == "" {
			errs = append(errs, "rabbit_url is required when broker=rabbitmq")
		}
	}
	if broker == "kafka" {
		if strings.TrimSpace(c.KafkaBrokers) == "" {
			errs = append(errs, "kafka_brokers is required when broker=kafka")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}