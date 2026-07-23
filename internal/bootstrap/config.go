package bootstrap

import (
	"fmt"
	"os"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	HTTP     HTTPConfig     `yaml:"http"`
	Postgres PostgresConfig `yaml:"postgres"`
	Redis    RedisConfig    `yaml:"redis"`
}

type AppConfig struct {
	New string `yaml:"config"`
	Env string `yaml:"env"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"database"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

func LoadConfig(configpath string) (*Config, error) {
	data, err := os.ReadFile(configPath)

	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if cfg.Postgres.Host == "" {
		return nil, fmt.Errorf("postgres.host is required")
	}

	if cfg.HTTP.Port == 0 {
		return nil, fmt.Errorf("http.port is required")
	}

	return &cfg, nil
}

// Why DSN method?
// Encapsulates connection string format.
// If you switch from Postgres to MySQL, only this method changes.
// Rest of code uses cfg.Postgres.DSN() without knowing the format.

func (c *PostgresConfig) DSN() string {
	return fmt.Sprintf(
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
		c.SSLMode,
	)
}

func (c *RedisConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
