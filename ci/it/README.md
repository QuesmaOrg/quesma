Quesma Integration Tests
========================

This directory contains integration tests for Quesma. 
These are simple, end-to-end tests that verify the functionality of Quesma using [Testcontainers library](https://testcontainers.com).




How to debug
============

There is a way to run these tests agains a local Quesma instance with debugger attached.

1. Set up a breakpoint in Quesma codebase.
2. Change the `debugQuesmaDuringTestRun` flag to `true` in `ci/it/testcases/utils.go`
3. Start an integration test (either with CLI or in your IDE using play button next to the test declaration).
   ...Test case execution will block and wait until you start Quesma manually in IDE in debug mode)
4. Start Quesma in Debug mode using `Debug Quesma ITs` Run Configuration in your IDE.