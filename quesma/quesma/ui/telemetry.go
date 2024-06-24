package ui

import (
	"encoding/json"
	"quesma/logger"
)

func (qmc *QuesmaManagementConsole) generateTelemetry() []byte {
	buffer := newBufferWithHead()
	buffer.Write(generateTopNavigation("telemetry"))
	buffer.Html(`<main id="telemetry">`)

	buffer.Html(`<h2>Telemetry</h2>`)
	buffer.Html("<pre>")

	stats, available := qmc.phoneHomeAgent.RecentStats()
	if available {
		asBytes, err := json.MarshalIndent(stats, "", "  ")

		if err != nil {
			logger.Error().Err(err).Msg("Error marshalling phone home stats")
			buffer.Html("Telemetry Stats are unable to be displayed. This is a bug.")
		} else {
			buffer.Html(string(asBytes))
		}

	} else {
		buffer.Html("Telemetry Stats are not available yet.")
	}

	buffer.Html("</pre>")

	buffer.Html("\n</body>")
	buffer.Html("\n</html>")
	return buffer.Bytes()
}
