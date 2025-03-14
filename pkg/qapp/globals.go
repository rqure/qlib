package qapp

import (
	"crypto/rand"
	"encoding/base64"
	"os"
	"sync"
	"time"
)

var applicationName string
var applicationInstanceId string

var tickRate = 100 * time.Millisecond

var mu = &sync.RWMutex{}

func GetName() string {
	mu.RLock()
	defer mu.RUnlock()
	return applicationName
}

func SetName(name string) {
	mu.Lock()
	defer mu.Unlock()
	applicationName = name
}

func GetApplicationInstanceId() string {
	mu.RLock()
	defer mu.RUnlock()

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

	if id == "" {
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
	mu.RLock()
	defer mu.RUnlock()
	return tickRate
}

func SetTickRate(rate time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	tickRate = rate
}
