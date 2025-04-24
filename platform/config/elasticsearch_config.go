// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

type ElasticsearchConfiguration struct {
	Url      *Url `koanf:"url"`
	AdminUrl *Url `koanf:"adminUrl"`

	User     string `koanf:"user"`
	Password string `koanf:"password"`

	ClientCertPath string `koanf:"clientCertPath"`
	ClientKeyPath  string `koanf:"clientKeyPath"`
	CACertPath     string `koanf:"caCertPath"`
}
