package main

import (
	"testing"
	"tp1/client/internal"
)

func TestGetLatestReport(t *testing.T) {
	internal.CompareWithExpectedReport("client_test.go")
}
