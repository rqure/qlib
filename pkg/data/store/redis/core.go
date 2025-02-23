package redis

import (
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Address  string
	Password string
}

type Core interface {
	SetClient(*redis.Client)
	GetClient() *redis.Client
	SetConfig(Config)
	GetConfig() Config
	GetKeyGen() KeyGenerator
}

type coreInternal struct {
	client *redis.Client
	config Config
	keygen KeyGenerator
}

func NewCore(config Config) Core {
	return &coreInternal{
		config: config,
		keygen: NewKeyGenerator(),
	}
}

func (me *coreInternal) SetClient(client *redis.Client) {
	me.client = client
}

func (me *coreInternal) GetClient() *redis.Client {
	return me.client
}

func (me *coreInternal) SetConfig(config Config) {
	me.config = config
}

func (me *coreInternal) GetConfig() Config {
	return me.config
}

func (me *coreInternal) GetKeyGen() KeyGenerator {
	return me.keygen
}
