package bootstrap

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App   AppConfig   `yaml:"app"`
	HTTP  HTTPConfig  `yaml:"http"`
	Mongo MongoConfig `yaml:"mongo"`
}

type AppConfig struct {
	New string `yaml:"config"`
	Env string `yaml:"env"`
}

type HTTPConfig struct {
	Port int `yaml:"port"`
}

type MongoConfig struct {
	URI            string `yaml:"uri"`
	Database       string `yaml:"database"`
	JobsCollection string `yaml:"jobs_collection"`
}

func LoadConfig(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)

	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(b, &cfg)
	return cfg, err
}
