// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package buildinfo

import (
	"fmt"
	"github.com/coreos/go-semver/semver"
	"github.com/goccy/go-json"
	"net/http"
	"time"
)

var Version = "development"
var BuildHash = ""
var BuildDate = ""

const GitHubLatestReleaseURL = "https://api.github.com/repos/quesma/quesma/releases/latest"

type ReleaseInfo struct {
	Name    string `json:"name"`
	TagName string `json:"tag_name"`
	// CreatedAt is the date of the commit used for the release, ref: https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release
	CreatedAt time.Time `json:"created_at"`
}

func (r *ReleaseInfo) IsNewerThanCurrentlyRunning() (bool, error) {
	// This is injected at build time, ignore IDE warning below
	if Version == "development" {
		return false, nil
	}
	return isNewer(r.Name, Version)
}

func isNewer(latest, current string) (bool, error) {
	vCurrent, vLatest := new(semver.Version), new(semver.Version)
	if err := vCurrent.Set(current); err != nil {
		return false, fmt.Errorf("error parsing current version: %v", err)
	}
	if err := vLatest.Set(latest); err != nil {
		return false, fmt.Errorf("error parsing latest version: %v", err)
	}
	return vCurrent.LessThan(*vLatest), nil
}

// CheckForTheLatestVersion obtains the latest release information from GitHub and compares it to the currently running version.
// It returns a user-facing message indicating whether the latest version is newer, the same, or if there was an error.
func CheckForTheLatestVersion() (updateAvailable bool, messageBanner string) {
	latestRelease, err := getLatestRelease()
	if err != nil {
		return false, fmt.Sprintf("Failed obtaining latest Quesma version from GitHub: %v", err)
	}
	shouldUpgrade, err := latestRelease.IsNewerThanCurrentlyRunning()
	if err != nil {
		return false, fmt.Sprintf("Failed comparing Quesma versions: %v", err)
	}
	if shouldUpgrade {
		return true, fmt.Sprintf("A new version of Quesma is available: %s", latestRelease.Name)
	} else {
		return false, fmt.Sprintf("You are running the latest version of Quesma: %s", Version)
	}
}

func getLatestRelease() (*ReleaseInfo, error) {
	resp, err := http.Get(GitHubLatestReleaseURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching latest release from GitHub: %v", err)
	}
	var releaseInfo ReleaseInfo
	if err = json.NewDecoder(resp.Body).Decode(&releaseInfo); err != nil {
		return nil, fmt.Errorf("error decoding JSON: %v", err)
	}
	defer resp.Body.Close()
	return &releaseInfo, nil
}
