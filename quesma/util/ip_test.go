// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
)

func TestBigIntToIpv6(t *testing.T) {
	testcases := []struct {
		ip       big.Int
		expected string
	}{
		{HexStringToBigInt(""), "::"},
		{HexStringToBigInt("ffff00000000"), "::ffff:0.0.0.0"},
		{HexStringToBigInt("ffffaaaaaaaa"), "::ffff:170.170.170.170"},
		{HexStringToBigInt("ffffc0a80b0c"), "::ffff:192.168.11.12"},
		{HexStringToBigInt("20010db8a4f8112a0000000000000000"), "2001:db8:a4f8:112a::"},
		{HexStringToBigInt("20010db885a308d313198a2e03707344"), "2001:db8:85a3:8d3:1319:8a2e:370:7344"},
	}
	for _, tc := range testcases {
		assert.Equal(t, tc.expected, BigIntToIpv6(tc.ip))
	}
}
