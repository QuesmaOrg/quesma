package telemetry

type dummyAgent struct {
}

func (d dummyAgent) Start() {
	// do nothing
}

func (d dummyAgent) Stop() {
	// do nothing
}

func (d dummyAgent) RecentStats() (recent PhoneHomeStats, available bool) {
	return PhoneHomeStats{}, false
}

func NewPhoneHomeDummyAgent() PhoneHomeAgent {
	return &dummyAgent{}
}
