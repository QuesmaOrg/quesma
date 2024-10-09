package ui

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"quesma/elasticsearch"
)

func (qmc *QuesmaManagementConsole) generateLoginForm() []byte {
	buffer := newBufferWithHead()
	buffer.Html(`<html>`)
	buffer.Html(`<head>`)
	buffer.Html(`<style>`)
	buffer.Html(`body { font-family: Courier, sans-serif; background-color: #2c2c2c; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }`)
	buffer.Html(`.login-form { background-color: #3c3c3c; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0, 0, 0, 0.1); font-family: Courier, sans-serif; }`)
	buffer.Html(`.login-form h2 { margin-bottom: 20px; color: #fff; font-family: Courier, sans-serif; }`)
	buffer.Html(`.login-form label { display: block; margin-bottom: 5px; color: #ccc; font-family: Courier, sans-serif; }`)
	buffer.Html(`.login-form input[type="text"], .login-form input[type="password"] { width: calc(100% - 10px); padding: 8px; margin-bottom: 10px; margin-right: 10px; border: 1px solid #555; border-radius: 4px; background-color: #555; color: #fff; font-family: Courier, sans-serif; }`)
	buffer.Html(`.login-form input[type="submit"] { width: 100%; padding: 10px; background-color: #444; border: none; border-radius: 4px; color: #fff; font-size: 16px; cursor: pointer; font-family: Courier, sans-serif; }`)
	buffer.Html(`.login-form input[type="submit"]:hover { background-color: #333; }`)
	buffer.Html(`</style>`)
	buffer.Html(`</head>`)
	buffer.Html(`<body>`)
	buffer.Html(`<div class="login-form">`)
	buffer.Html(`<h2>Login</h2>`)
	buffer.Html(`<p style="color: #ccc;">Log in to Quesma admin console using your Elasticsearch credentials</p>`)
	buffer.Html(`<form action="/login-with-elasticsearch" method="post">`)
	buffer.Html(`<label for="username">Username:</label>`)
	buffer.Html(`<input type="text" id="username" name="username" placeholder="Enter your Elasticsearch username">`)
	buffer.Html(`<label for="password">Password:</label>`)
	buffer.Html(`<input type="password" id="password" name="password" placeholder="Enter your Elasticsearch password">`)
	buffer.Html(`<input type="submit" value="Login">`)
	buffer.Html(`</form>`)
	buffer.Html(`</div>`)
	buffer.Html(`</body>`)
	buffer.Html(`</html>`)
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) HandleElasticsearchLogin(writer http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		writer.Header().Set("Content-Type", "text/html")
		writer.Write(qmc.generateLoginForm())
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
