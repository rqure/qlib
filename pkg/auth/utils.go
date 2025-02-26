package auth

import "os"

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}

func generateRealmConfigFromEnv() *RealmConfig {
	return &RealmConfig{
		AdminUsername: getEnvOrDefault("Q_KEYCLOAK_ADMIN_USERNAME", "admin"),
		AdminPassword: getEnvOrDefault("Q_KEYCLOAK_ADMIN_PASSWORD", "admin"),
		MasterRealm:   getEnvOrDefault("Q_KEYCLOAK_MASTER_REALM", "master"),
	}
}
