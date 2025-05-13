[INTERNAL ONLY] Checking how Quesma MVP works with provided Kibana dashboards
======================================================================

:warning: **This is an internal-only document.** Despite our open-source DNA, procedures described here cannot be replayed 
without access to internal resources.


Let's say someone provided you with some Kibana dashboards exported as NDJSON files.

1. Place dashboard json files in `bin/unjson-dashboards/dashboards`
2. Run `local-dev-chrome.yml` docker compose file from `quesma-examples` repository (private repo)
3. Open `bin/unjson-dashboards/main.go` in your IDE \
   We will be running steps by changing `if` statement condition in that file and running go program.  

There are 4 stages there:
* `extractMappings()` - figures out indices/mappings from the dashboards and writes them to `bin/unjson-dashboards/mappings` directory
* `ingestAll()` - generates some test data according to the mappings and ingests it using Elasticsearch API to ClickHouse via Quesma  
* `createDataViews()/importDashboards()` - creates relevant data views in Kibana
* `visitDashboards()` - opens headless chrome which makes screenshot of these dashboards, saves them in `bin/unjson-dashboards/screenshots` and adds `index.html` so that these can be browsed in a convenient manner.
