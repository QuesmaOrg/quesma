// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"math/big"
	"net"
	"net/netip"
)

func IntToIpv4(ip uint32) string {
	result := make(net.IP, 4)
	result[0] = byte(ip >> 24)
	result[1] = byte(ip >> 16)
	result[2] = byte(ip >> 8)
	result[3] = byte(ip)
	return result.String()
}

// BigIntToIpv6 converts a big.Int to an IPv6 string.
// I don't think there's any library that does exactly this, have checked a few.
func BigIntToIpv6(ip big.Int) string {
	const ipv6len = 16
	ipBytes := ip.Bytes()
	nonZeroSuffixLen := len(ipBytes)
	resultAsBytes := [ipv6len]byte{}
	for i := range nonZeroSuffixLen {
		resultAsBytes[i+ipv6len-nonZeroSuffixLen] = ipBytes[i]
	}
	return netip.AddrFrom16(resultAsBytes).String()
}

// HexStringToBigInt converts a hex string (e.g. "ffffc0a80b0c", or "20010db8a4f8112a0000000000000000") to a big.Int
// Often useful when dealing with IPv6s.
func HexStringToBigInt(s string) (i big.Int) {
	i.SetString(s, 16)
	return i
}
