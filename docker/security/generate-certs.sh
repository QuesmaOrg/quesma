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

# some openssl magic
openssl genrsa --passout pass:1234 -aes256 -out ca.key 4096
openssl req --passin pass:1234 -new -key ca.key -x509 -out ca.crt -days 3650 -batch -subj  "/C=pl/ST=W/L=W/O=Quesma CA/OU=ENG/CN=elastic"
openssl req -new -nodes -newkey rsa:4096 -keyout keyout.key -out out.req -batch -subj  "/C=pl/ST=W/L=W/O=Quesma CA/OU=ENG/CN=elastic" -config <(cat /etc/ssl/openssl.cnf <(printf "[SAN]\nsubjectAltName=DNS:elasticsearch"))
openssl x509 -req -in out.req -CA ca.crt -CAkey ca.key --passin pass:1234 -CAcreateserial -out key.crt -days 3650 -sha256  -extfile <(printf "subjectAltName=DNS:elasticsearch")

mkdir -p /setup-ssl/ca
mkdir -p /setup-ssl/es.local/

cp ca.crt /setup-ssl/ca/ca.crt
cp key.crt /setup-ssl/es.local/es.local.crt
cp keyout.key /setup-ssl/es.local/es.local.key



