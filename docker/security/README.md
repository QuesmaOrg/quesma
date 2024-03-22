This directory should contain the keys and certificates required by Elasticsearch with security enabled.

Grab `certificate-bundle.zip` from 1Password (look up `Elasticsearch self-signed keys/certs for local debugging`) and unzip it here:
```
security % unzip certificate-bundle.zip 
Archive:  certificate-bundle.zip
   creating: ca/
  inflating: ca/ca.crt               
  inflating: ca/ca.key               
   creating: es.local/
  inflating: es.local/es.local.crt   
  inflating: es.local/es.local.key   
```

You can also generate your own keys and certificates by docker-exec'ing into the container and running
`./bin/elasticsearch-certutil http` and responding to the prompts there (domain name, key passphrase, etc).


**NOTE:** Of course any client validation will fail as the root certificate is not trusted.
Remember to use `curl -k` and any other client options to disable SSL verification.