package telemetry

type emptyTimer struct {
}

type emptySpan struct {
}

func (d emptySpan) End(err error) {
}

func (d emptyTimer) Begin() Span {
	return emptySpan{}
}

func (d emptyTimer) Aggregate() DurationStats {
	return DurationStats{}
}

type emptyAgent struct {
}

func (d emptyAgent) Start() {
	// do nothing
}

func (d emptyAgent) Stop() {
	// do nothing
}

func (d emptyAgent) RecentStats() (recent PhoneHomeStats, available bool) {
	return PhoneHomeStats{}, false
}

func (d emptyAgent) ClickHouseQueryDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ClickHouseInsertDuration() DurationMeasurement {
	return &emptyTimer{}
}

func (d emptyAgent) ElkasticQueryDuration() DurationMeasurement {
	return &emptyTimer{}
}

func NewPhoneHomeEmptyAgent() PhoneHomeAgent {
	return &emptyAgent{}
}
