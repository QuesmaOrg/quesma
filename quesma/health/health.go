package health

import "fmt"

type Checker interface {
	CheckHealth() Status
}

type Status struct {
	Status  string
	Message string
	Tooltip string
}

func (s Status) String() string {
	return fmt.Sprintf("%s: %s", s.Status, s.Message)
}
