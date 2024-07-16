// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package config

import (
	"fmt"
	"strings"
)

type IndexMappingsConfiguration struct {
	Name     string   `koanf:"name"`
	Mappings []string `koanf:"sourceIndexes"`
}

func (imc IndexMappingsConfiguration) String() string {
	var str = fmt.Sprintf("\n\t\t%s",
		imc.Name,
	)
	if len(imc.Mappings) > 0 {
		str = fmt.Sprintf("%s <- %s", str, strings.Join(imc.Mappings, ", "))
	}
	return str
}
