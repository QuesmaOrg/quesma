This directory should contain the keys and certificates required by Elasticsearch with security enabled.

Certificates are generated on fly by "setup-ssl" container and stored in this directory.
See `local-debug-with-security.yml` for details.


**NOTE:** Of course any client validation will fail as the root certificate is not trusted.
Remember to use `curl -k` and any other client options to disable SSL verification.