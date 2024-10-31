// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"net/url"
)

type Url url.URL

func (u *Url) ToUrl() *url.URL {
	return (*url.URL)(u)
}

func (u *Url) UnmarshalText(text []byte) error {
	urlValue, err := url.Parse(string(text))
	if err != nil {
		return err
	}
	if len(urlValue.Scheme) == 0 {
		return fmt.Errorf("URL scheme (e.g. http:// or clickhouse://) is missing from the provided URL: %s", urlValue)
	}
	if len(urlValue.Port()) == 0 {
		return fmt.Errorf("URL port (e.g. 8123 in 'http://localhost:8123') is missing from the provided URL: %s", urlValue)
	}
	*u = Url(*urlValue)
	return nil
}

func (u *Url) Hostname() string {
	urlValue := url.URL(*u)
	return urlValue.Hostname()
}

func (u *Url) Port() string {
	urlValue := url.URL(*u)
	return urlValue.Port()
}

func (u *Url) String() string {
	urlValue := url.URL(*u)
	return urlValue.String()
}

func (u *Url) JoinPath(elem ...string) *url.URL {
	urlValue := url.URL(*u)
	return urlValue.JoinPath(elem...)
}
