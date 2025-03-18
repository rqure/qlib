package qauth

import (
	"github.com/Nerzal/gocloak/v13"
)

type Core interface {
	GetClient() *gocloak.GoCloak
}

type core struct {
	client *gocloak.GoCloak
	config *coreConfig
}

type coreConfig struct {
	baseURL string
}

type CoreOption func(*coreConfig)

func BaseURL(url string) CoreOption {
	return func(c *coreConfig) {
		c.baseURL = url
	}
}

func NewCore(opts ...CoreOption) Core {
	config := &coreConfig{}

	defaultOpts := []CoreOption{
		BaseURL(getEnvOrDefault("Q_KEYCLOAK_BASE_URL", "http://keycloak:8080/")),
	}

	for _, opt := range defaultOpts {
		opt(config)
	}

	for _, opt := range opts {
		opt(config)
	}

	return &core{
		client: gocloak.NewClient(config.baseURL),
		config: config,
	}
}

func (me *core) GetClient() *gocloak.GoCloak {
	return me.client
}
