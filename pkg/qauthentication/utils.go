package qauthentication

import (
	"os"
	"strings"
)

func getEnvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	return value
}

// getBoolEnvOrDefault gets a boolean environment variable or returns the default value
func getBoolEnvOrDefault(key string, defaultValue bool) bool {
	value := getEnvOrDefault(key, "")
	if value == "" {
		return defaultValue
	}
	return strings.ToLower(value) == "true" || value == "1" || value == "yes"
}

// mergeStringArrays combines two string arrays without duplicates
func mergeStringArrays(a, b []string) []string {
	// Use a map to track unique strings
	merged := make(map[string]bool)

	// Add all strings from both arrays to the map
	for _, str := range a {
		merged[str] = true
	}

	for _, str := range b {
		merged[str] = true
	}

	// Convert map keys back to slice
	result := make([]string, 0, len(merged))
	for str := range merged {
		result = append(result, str)
	}

	return result
}
