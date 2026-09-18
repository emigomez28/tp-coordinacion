package middleware

import "fmt"

func GetURI(hostname string, port int) string {
	return fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port)
}
