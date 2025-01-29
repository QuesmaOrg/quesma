// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package util

import (
	"fmt"
	"strconv"
)

type Port uint16

func (p *Port) UnmarshalText(text []byte) error {
	var portValue uint64
	if val, err := strconv.ParseUint(string(text), 10, 16); err != nil {
		return err
	} else {
		portValue = val
	}
	if portValue > 65535 { // no value of type uint64 is less than 0 (SA4003)
		return fmt.Errorf("invalid port number: %s", text)
	}
	*p = Port(portValue)
	return nil
}
