package env

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)


type AccessKeys struct {
	Endpoint string
	AccessKeyID string
	SecretAccessKey string
}

func LoadKeys() *AccessKeys {
	if err := godotenv.Load(); err != nil {
			log.Fatal("Failed to Load envs")
		}
	Keys := &AccessKeys{
		Endpoint: os.Getenv("ENDPOINT"),
		AccessKeyID: os.Getenv("ACCESSKEYID"),
		SecretAccessKey: os.Getenv("SECRETACCESSKEY"),
	}
	fmt.Println("Successfully Loaded Keys")
	fmt.Println(Keys)

	return Keys
}
