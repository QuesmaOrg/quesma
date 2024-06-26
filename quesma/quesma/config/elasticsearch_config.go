// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

type ElasticsearchConfiguration struct {
	Url      *Url   `koanf:"url"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	Call     bool   `koanf:"call"`
	AdminUrl *Url   `koanf:"adminUrl"`
}
