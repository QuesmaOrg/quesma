#! /bin/bash

#
# This script generates the certificates for the elasticsearch cluster
# It's called by local-debug-with-security.yml
#


if [ -f /setup-ssl/ca/ca.crt ]
then
  echo "Certificates already generated"
  exit 0
fi


cd /tmp

echo "Generating certificates"

mkdir -p /setup-ssl/ca


echo "Certificate Authority"

openssl genrsa --passout pass:1234 -aes256 -out ca.key 4096
openssl req --passin pass:1234 -new -key ca.key -x509 -out ca.crt -days 3650 -batch -subj  "/C=pl/ST=W/L=W/O=Quesma CA/OU=ENG/CN=quesma"

cp ca.crt /setup-ssl/ca/ca.crt

echo "Elastic Certificate"

mkdir -p /setup-ssl/es.local/

# some openssl magic
openssl req -new -nodes -newkey rsa:4096 -keyout keyout.key -out out.req -batch -subj  "/C=pl/ST=W/L=W/O=Quesma CA/OU=ENG/CN=elastic" -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=DNS:elasticsearch"))
openssl x509 -req -in out.req -CA ca.crt -CAkey ca.key --passin pass:1234 -CAcreateserial -out key.crt -days 3650 -sha256  -extfile <(printf "subjectAltName=DNS:elasticsearch")

mv key.crt /setup-ssl/es.local/es.local.crt
mv keyout.key /setup-ssl/es.local/es.local.key


echo "Clickhouse Certificate"

mkdir -p /setup-ssl/clickhouse/

openssl req -new -nodes -newkey rsa:4096 -keyout keyout.key -out out.req -batch -subj  "/C=pl/ST=W/L=W/O=Quesma CA/OU=ENG/CN=clickhouse" -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=DNS:clickhouse"))
openssl x509 -req -in out.req -CA ca.crt -CAkey ca.key --passin pass:1234 -CAcreateserial -out key.crt -days 3650 -sha256  -extfile <(printf "subjectAltName=DNS:clickhouse")

mv key.crt /setup-ssl/clickhouse/clickhouse.crt
mv keyout.key /setup-ssl/clickhouse/clickhouse.key





