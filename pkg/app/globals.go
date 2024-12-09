package app

import (
	"crypto/rand"
	"encoding/base64"
	"os"
)

var applicationName string
var applicationInstanceId string

func GetApplicationName() string {
	return applicationName
}

func SetApplicationName(name string) {
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
