package app

var applicationName string

func GetApplicationName() string {
	return applicationName
}

func SetApplicationName(name string) {
	applicationName = name
}
