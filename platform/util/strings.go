package util

import "fmt"

func PrettyTestName(name string, idx int) string {
	return fmt.Sprintf("%s(%d)", name, idx)
}
