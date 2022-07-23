package utils

import "fmt"

func SizeKB(sz uint64) string {
	v := float32(sz) / 1024.0
	return fmt.Sprintf("%.2fKB", v)
}

func SizeMB(sz uint64) string {
	v := float32(sz) / 1024.0 / 1024.0
	return fmt.Sprintf("%.2fMB", v)
}

func SizeGB(sz uint64) string {
	v := float32(sz) / 1024.0 / 1024.0 / 1024.0
	return fmt.Sprintf("%.2fGB", v)
}
