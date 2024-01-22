package util

func Truncate(body string) string {
	if len(body) < 70 {
		return body
	}
	return body[:70]
}
