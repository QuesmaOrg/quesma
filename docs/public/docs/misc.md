### Feedback and support
In case of any issues, questions or feedback, please reach out to us at [support@quesma.com](mailto:support@quesma.com).

### Debugging interface
Quesma exposes a debugging interface on port `9999`. It can be accessed by web browser `http://localhost:9999`.

### Telemetry collection

Quesma collects telemetry data about the usage of the service. It is subset of data visible in debugging interface.

This data is used to improve the product and is not shared with any third parties.

Telemetry data consists of:
* Quesma environment information like components versions and runtime stats. \
 **Example entry:**
  <details>

  ```
    {
    "started_at": 1713180071,
    "hostname": "MacBook-Pro.local",
    "quesma_version": "development",
    "instanceId": "438c42a6-fb1a-11ee-bcc4-b66e58b1f280",
    "clickhouse": {
      "status": "ok",
      "number_of_rows": 14725105,
      "disk_space": 17047512,
      "open_connection": 2,
      "max_open_connection": 0,
      "server_version": "23.12.2.59"
    },
    "elasticsearch": {
      "status": "ok",
      "number_of_docs": 0,
      "size": 747,
      "server_version": "8.11.1"
    },
    "clickhouse_queries": {
      "count": 0,
      "avg_time_sec": 0,
      "failed": 12,
      "over_thresholds": {
        "1": 0,
        "10": 0,
        "30": 0,
        "5": 0,
        "60": 0
      },
      "percentiles": {
        "25": 0,
        "5": 0,
        "50": 0,
        "75": 0,
        "95": 0
      }
    },
    "clickhouse_inserts": {
      "count": 0,
      "avg_time_sec": 0,
      "failed": 0,
      "over_thresholds": {
        "1": 0,
        "10": 0,
        "30": 0,
        "5": 0,
        "60": 0
      },
      "percentiles": {
        "25": 0,
        "5": 0,
        "50": 0,
        "75": 0,
        "95": 0
      }
    },
    "elastic_queries": {
      "count": 38,
      "avg_time_sec": 0.002649310236842105,
      "failed": 0,
      "over_thresholds": {
        "1": 0,
        "10": 0,
        "30": 0,
        "5": 0,
        "60": 0
      },
      "percentiles": {
        "25": 0.000872708,
        "5": 0.000657125,
        "50": 0.001412542,
        "75": 0.005473208,
        "95": 0.007779666
      }
    },
    "top_user_agents": [
      "Kibana/8.11.1"
    ],
    "runtime": {
      "memory_used": 8296328,
      "memory_available": 38654705664
    },
    "number_of_panics": 0,
    "report_type": "on-schedule",
    "taken_at": 1713180101
  }
  ```
  </details>
* Quesma logs

### Troubleshooting

This section provides a list of issues and their solutions. 

#### Quesma eats all resources (CPU, memory)

There is a profiling endpoint available at `http://localhost:9999/debug/pprof/`. It can be used to profile the Quesma.

1. Fetch a CPU and heap profile (it may take a few seconds)
```bash
curl http://localhost:9999/debug/pprof/profile > profile.out
```
2. Fetch a heap profile
```bash
curl http://localhost:9999/debug/pprof/heap > heap.out
```
3. Send files to Quesma support for further analysis.


If you want to analyze the profile locally, follow these steps:
1. Make sure you have `GoLang` SDK installed
2. Analyze the CPU profile
```bash
go tool pprof -http=:8082 profile.out
```
3. Analyze the heap 
```bash
go tool pprof -http=:8082 heap.out
```





