package bootstrap

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	HTTP     HTTPConfig     `yaml:"http"`
	Postgres PostgresConfig `yaml:"postgres"`
}

type AppConfig struct {
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
	SSLMode  string `yaml:"sslmode"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.HTTP.Port == 0 {
		return nil, fmt.Errorf("http.port is required")
	}
	if cfg.Postgres.Host == "" || cfg.Postgres.User == "" || cfg.Postgres.Database == "" {
		return nil, fmt.Errorf("postgres host/user/database are required")
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
	}

	return &cfg, nil
}

func (c PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host,
		c.Port,
		c.User,
		c.Password,
		c.Database,
		c.SSLMode,
	)
}
