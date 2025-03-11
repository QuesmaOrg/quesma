// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"
)

const url = "http://mitmproxy:8080/logs-generic-default/_doc"

func main() {
	hostNames := []string{"zeus", "cassandra", "hercules",
		"oracle", "athena", "jupiter", "poseidon", "hades", "artemis", "apollo", "demeter",
		"dionysus", "hephaestus", "hermes", "hestia", "iris", "nemesis", "pan", "persephone", "prometheus", "selen"}

	serviceNames := []string{"frontend", "backend", "database", "cache", "queue", "monitoring", "loadbalancer", "proxy",
		"storage", "auth", "api", "web", "worker", "scheduler", "cron", "admin", "service", "gateway", "service", "service", "service"}

	sourceNames := []string{"kubernetes", "ubuntu", "debian", "centos", "redhat", "fedora", "arch", "gentoo", "alpine", "suse",
		"rhel", "coreos", "docker", "rancher", "vmware", "xen", "hyperv", "openstack", "aws", "gcp", "azure", "digitalocean"}

	severityNames := []string{"info", "info", "info", "info", "info", "info", "warning", "error", "critical", "debug", "debug", "debug"}

	messageNames := []string{"User logged in", "User logged out", "User created", "User deleted", "User updated",
		"User password changed", "User password reset", "User password reset requested", "User password reset failed"}

	for {
		time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)

		body, err := json.Marshal(map[string]string{
			// Please keep using OpenTelemetry names for the fields:
			// https://opentelemetry.io/docs/specs/semconv/resource/
			"@timestamp":   time.Now().Format("2006-01-02T15:04:05.999Z"),
			"message":      messageNames[rand.Intn(len(messageNames))],
			"severity":     severityNames[rand.Intn(len(severityNames))],
			"source":       sourceNames[rand.Intn(len(sourceNames))],
			"service.name": serviceNames[rand.Intn(len(serviceNames))],
			"host.name":    hostNames[rand.Intn(len(hostNames))],
		})

		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))

		if err != nil {
			log.Fatal(err)
		}

		resp.Body.Close()
	}
}
