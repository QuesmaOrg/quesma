// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import "net"

func IntToIpv4(ip uint32) string {
	result := make(net.IP, 4)
	result[0] = byte(ip >> 24)
	result[1] = byte(ip >> 16)
	result[2] = byte(ip >> 8)
	result[3] = byte(ip)
	return result.String()
}
