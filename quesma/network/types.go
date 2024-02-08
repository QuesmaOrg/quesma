package network

import (
	"fmt"
	"strconv"
)

type Port uint16

func ParsePort(port string) (Port, error) {
	tcpPortInt, err := strconv.Atoi(port)
	if err != nil {
		return 0, fmt.Errorf("error parsing tcp port %s: %v", port, err)
	}
	if tcpPortInt < 0 || tcpPortInt > 65535 {
		return 0, fmt.Errorf("invalid port number: %s", port)
	}
	return Port(tcpPortInt), nil
}
