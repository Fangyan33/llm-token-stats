//go:build !tinygo

package main

import "os"

func logLevelEnvironment() string { return os.Getenv("LOG_LEVEL") }
