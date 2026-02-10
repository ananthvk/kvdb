package utils

import "fmt"

func GetFileName(identifier int) string {
	return fmt.Sprintf("%010d", identifier)
}

func GetDataFileName(identifier int) string {
	return fmt.Sprintf("%010d.dat", identifier)
}
