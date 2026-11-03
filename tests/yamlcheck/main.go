// yamlcheck is a test-only YAML reader for Python rendering contract tests.
package main

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

func main() {
	b, e := io.ReadAll(os.Stdin)
	if e != nil {
		panic(e)
	}
	var v any
	if e = yaml.Unmarshal(b, &v); e != nil {
		panic(e)
	}
	if e = json.NewEncoder(os.Stdout).Encode(v); e != nil {
		panic(e)
	}
}
