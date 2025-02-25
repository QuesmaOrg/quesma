// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package ui

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/elasticsearch"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"net/http"
)

func (qmc *QuesmaManagementConsole) generateLoginForm() []byte {
	buffer := newBufferWithHead()
	buffer.Html(`<html>`)
	buffer.Html(`<body>`)
	buffer.Html(`<div class="login-screen">`)
	buffer.Html(`<div class="login-form">`)
	buffer.Html(`<h2>Login</h2>`)
	buffer.Html(`<p style="color: #ccc;">Log in to Quesma admin console using your Elasticsearch credentials</p>`)
	buffer.Html(`<form action="`).Text(loginWithElasticSearch).Html(`" method="post">`)
	buffer.Html(`<label for="username">Username:</label>`)
	buffer.Html(`<input type="text" id="username" name="username" placeholder="Enter your Elasticsearch username" autofocus>`)
	buffer.Html(`<label for="password">Password:</label>`)
	buffer.Html(`<input type="password" id="password" name="password" placeholder="Enter your Elasticsearch password">`)
	buffer.Html(`<input type="submit" value="Login">`)
	buffer.Html(`</form>`)
	buffer.Html(`</div>`)
	buffer.Html(`</div>`)
	buffer.Html(`</body>`)
	buffer.Html(`</html>`)
	return buffer.Bytes()
}

func (qmc *QuesmaManagementConsole) HandleElasticsearchLogin(writer http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		if isAlreadyAuthenticated(req) {
			http.Redirect(writer, req, "/dashboard", http.StatusSeeOther)
			return
		}
		writer.Header().Set("Content-Type", "text/html")
		writer.Header().Set("HX-Redirect", loginWithElasticSearch)
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
			logger.Warn().Msgf("Invalid credentials for user [%s], could not login with Elasticsearch", username)
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
