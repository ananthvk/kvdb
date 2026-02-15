package utils

import "fmt"

func GetFileName(identifier int) string {
	return fmt.Sprintf("%010d", identifier)
}

func GetDataFileName(identifier int) string {
	return fmt.Sprintf("%010d.dat", identifier)
}

func GetHintFileName(identifier int) string {
	return fmt.Sprintf("%010d.hint", identifier)
}
