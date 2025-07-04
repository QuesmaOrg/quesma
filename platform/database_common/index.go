// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package database_common

type IndexStatement string

func (s IndexStatement) Statement() string {
	return string(s)
}

func GetIndexStatement(column string) IndexStatement {
	switch column {
	case "severity":
		return "INDEX severity_idx severity TYPE set(25) GRANULARITY 4"
	case "body":
		return "INDEX body_idx body TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4"
	case "trace_flags":
		return "INDEX trace_flags_idx trace_flags TYPE bloom_filter GRANULARITY 4"
	case "id":
		return "INDEX id_idx id TYPE minmax GRANULARITY 1"
	case "event_name":
		return "INDEX event_name_idx event_name TYPE minmax GRANULARITY 1"
	}
	return ""
}
