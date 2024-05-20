package main

import "database/sql"

type SQLDatabase struct {
	db *sql.DB
}

func (d *SQLDatabase) Query(query JSON) ([]JSON, error) {

	sqlQuery := query["query"].(string)

	rows, err := d.db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var docs []JSON

	for rows.Next() {
		doc := make(JSON)

		row := make([]any, len(cols))
		for i := range row {
			row[i] = new(interface{})
		}
		err = rows.Scan(row...)
		if err != nil {
			return nil, err
		}

		for i, col := range cols {
			doc[col] = *row[i].(*interface{})
		}

		docs = append(docs, doc)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return docs, nil
}
