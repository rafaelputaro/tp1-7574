package main

import (
	"testing"
	"tp1/client/internal"
)

func TestGetLatestReport(t *testing.T) {
	internal.CompareReport(".", "client_test.go")
}
