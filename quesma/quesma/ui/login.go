package ui

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"quesma/elasticsearch"
)

func (qmc *QuesmaManagementConsole) HandleElasticsearchLogin(writer http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		writer.Header().Set("Content-Type", "text/html")
		writer.Write([]byte(`
            <html>
            <body>
                <form action="/login-with-elasticsearch" method="post">
                    <label for="username">Username:</label>
                    <input type="text" id="username" name="username"><br>
                    <label for="password">Password:</label>
                    <input type="password" id="password" name="password"><br>
                    <input type="submit" value="Login">
                </form>
            </body>
            </html>
        `))
	} else if req.Method == http.MethodPost {
		username := req.FormValue("username")
		password := req.FormValue("password")
		if qmc.isValidElasticsearchUser(username, password) {
			session, _ := store.Get(req, quesmaSessionName)
			session.Values["userID"] = username
			session.Save(req, writer)
			http.Redirect(writer, req, "/dashboard", http.StatusSeeOther)
		} else {
			http.Error(writer, "Invalid credentials", http.StatusUnauthorized)
		}
	} else {
		http.Error(writer, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (qmc *QuesmaManagementConsole) isValidElasticsearchUser(username, password string) bool {
	ctx := context.Background()
	authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", username, password)))
	return elasticsearch.NewSimpleClient(&qmc.cfg.Elasticsearch).Authenticate(ctx, authHeader)
}
