// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go deviceLogGenerator()
	go windowsLogGeneratorAssetBased()
	go windowsLogGeneratorRandom()

	<-sig
}
