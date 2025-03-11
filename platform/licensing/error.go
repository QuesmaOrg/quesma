// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import "fmt"

const (
	errorMessage = `There's been license violation detected. Please contact us at:
		support@quesma.com`
)

func PanicWithLicenseViolation(initialErr error) {
	panic(fmt.Sprintf("%v\n%s", initialErr, errorMessage))
}
