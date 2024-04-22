## Quality Assurance setup

Quesma has end-to-end tests which rely on recording and replaying HTTP requests.

See [Architectural decision record](../../quesma/adr/2_end_to_end_testing.md) for background.

### Running end-to-end tests
From top folder:
```bash
bin/up.sh qa/replay-elastic
```
Check logs of the `qa_replay-traffic` container for the results.

Shutdown it with:
```bash
bin/down.sh qa/replay-elastic
```

### Recording new tests
From top folder:
```bash
bin/up.sh qa/record-kibana
```

Go to [local Kibana](http://localhost:5601) and perform some actions.

Shutdown it with:
```bash
bin/down.sh qa/record-kibana
```

For local testing, copy the file `mitmproxy/requests/recorded_traffic.json` to `replay-traffic/data/`.
Then, rerun the end-to-end tests.

If you are happy with the results, upload it to [repository](https://console.cloud.google.com/storage/browser/elastic-sample-data/version-2024-04-22/traffic)
and make it publicly accessible. Add the corresponding download in `replay-traffic/Dockerfile`.

### Customizations
See `mitmproxy/record.py` for record logic. Currently, only URL paths starting with `/kibana_sample_data` are recorded.

See `replay-traffic/replay.py` for replay logic. Currently, I parse JSON to do comparisons but ignore some fields.


