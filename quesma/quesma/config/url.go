// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import "net/url"

type Url url.URL

func (u *Url) ToUrl() *url.URL {
	return (*url.URL)(u)
}

func (u *Url) UnmarshalText(text []byte) error {
	urlValue, err := url.Parse(string(text))
	if err != nil {
		return err
	}
	*u = Url(*urlValue)
	return nil
}

func (u *Url) Hostname() string {
	urlValue := url.URL(*u)
	return urlValue.Hostname()
}

func (u *Url) String() string {
	urlValue := url.URL(*u)
	return urlValue.String()
}

func (u *Url) JoinPath(elem ...string) *url.URL {
	urlValue := url.URL(*u)
	return urlValue.JoinPath(elem...)
}
