package catch_all_logs

import "strings"

const Enabled = true

const TableName = "catch_all_logs"
const IndexNameColumn = "__quesma_index_name"

func MangeIndexName(indexName string) string {
	return strings.ReplaceAll(indexName, ".", "__")
}
