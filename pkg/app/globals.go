package app

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"time"
)

var applicationName string
var applicationInstanceId string
var applicationCtx context.Context
var applicationCancel context.CancelFunc

var tickRate = 100 * time.Millisecond

func GetName() string {
	return applicationName
}

func SetName(name string) {
	applicationName = name
}

func GetApplicationInstanceId() string {
	if applicationInstanceId == "" {
		applicationInstanceId = PrepareApplicationInstanceId()
	}

	return applicationInstanceId
}

func PrepareApplicationInstanceId() string {
	id := ""

	if os.Getenv("Q_IN_DOCKER") != "" {
		id = os.Getenv("HOSTNAME")
	}

	if applicationInstanceId == "" {
		id = randomString()
	}

	return id
}

func randomString() string {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return ""
	}

	r := base64.StdEncoding.EncodeToString(randomBytes)
	return r[:len(r)-1]
}

func GetTickRate() time.Duration {
	return tickRate
}

func SetTickRate(rate time.Duration) {
	tickRate = rate
}

func InitCtx() {
	applicationCtx, applicationCancel = context.WithCancel(context.Background())
}

func GetCtx() context.Context {
	return applicationCtx
}

func GetCancel() context.CancelFunc {
	return applicationCancel
}
