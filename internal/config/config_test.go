package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaultsAndEnv(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("WORKER_COUNT", "4")
	t.Setenv("MAX_RETRIES", "5")
	t.Setenv("CONFIG_FILE", "")

	_ = os.Unsetenv("CONFIG_FILE")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "redis:6379", cfg.RedisAddr)
	require.Equal(t, 4, cfg.WorkerCount)
	require.Equal(t, 5, cfg.MaxRetries)
}

func TestLoadFailsOnBadWorkerCount(t *testing.T) {
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("WORKER_COUNT", "0")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "worker_count")
}

func TestLoadFailsOnBadDuration(t *testing.T) {
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("VISIBILITY_TIMEOUT", "not-a-duration")

	_, err := Load()
	require.Error(t, err)
	require.Contains(t, err.Error(), "VISIBILITY_TIMEOUT")
}

func TestLoadFileThenEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"redis_addr": "file-redis:6379",
		"worker_count": 2,
		"visibility_timeout": "45s"
	}`
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	t.Setenv("CONFIG_FILE", path)
	t.Setenv("REDIS_ADDR", "env-redis:6379")
	t.Setenv("WORKER_COUNT", "")
	_ = os.Unsetenv("WORKER_COUNT")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "env-redis:6379", cfg.RedisAddr)
	require.Equal(t, 2, cfg.WorkerCount)
	require.Equal(t, 45*time.Second, cfg.VisibilityTimeout)
}

func TestValidateRejectsEmptyRedis(t *testing.T) {
	cfg := defaults()
	cfg.RedisAddr = ""
	err := cfg.Validate()
	require.Error(t, err)
}

func TestLoad_BrokerFromEnv(t *testing.T) {
	t.Setenv("BROKER", "rabbitmq")
	t.Setenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("REDIS_ADDR", "localhost:6379")

	cfg, err := Load()
	require.NoError(t, err)
	require.Equal(t, "rabbitmq", cfg.Broker)
	require.Contains(t, cfg.RabbitURL, "amqp://")
}

func TestValidate_RabbitRequiresURL(t *testing.T) {
	cfg := defaults()
	cfg.Broker = "rabbitmq"
	cfg.RabbitURL = ""
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "rabbit_url")
}

func TestValidate_UnknownBroker(t *testing.T) {
	cfg := defaults()
	cfg.Broker = "sqs"
	err := cfg.Validate()
	require.Error(t, err)
}