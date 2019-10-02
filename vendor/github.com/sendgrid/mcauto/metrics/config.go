package metrics

import (
	log "github.com/sirupsen/logrus"
)

// AWSConfig sets options for the metrics logger
type AWSConfig struct {
	LogLevel   log.Level
	Region     string
	Namespace  string
	Dimensions map[string]string
}

// NewAWSConfig returns a default configuration
func NewAWSConfig(namespace string, region string) AWSConfig {
	return AWSConfig{
		LogLevel:   log.InfoLevel,
		Region:     region,
		Namespace:  namespace,
		Dimensions: make(map[string]string),
	}
}
