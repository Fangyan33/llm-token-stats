package gatewayconfig

import (
	"encoding/json"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestStructConfigurationAndStrictFailures(t *testing.T) {
	b, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	json.Unmarshal(b, &raw)
	message, err := structpb.NewStruct(raw)
	if err != nil {
		t.Fatal(err)
	}
	text, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(text)
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatalf("Struct differs: %v\n%s", err, text)
	}
	for _, bad := range [][]byte{text[:len(text)-1], append(append([]byte{}, text...), 0), []byte("schema_version: [bad"), []byte("{\"schema_version\":1,}"), []byte(strings.Repeat("x", 65537))} {
		if _, err := Decode(bad); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	raw["schema_version"] = "1"
	message, _ = structpb.NewStruct(raw)
	text, _ = proto.Marshal(message)
	if _, err := Decode(text); err == nil {
		t.Fatal("wrong field type accepted")
	}

}

func TestRenderedYAMLRoundTrip(t *testing.T) {
	dir := t.TempDir()
	artifact := filepath.Join(dir, "p.wasm")
	out := filepath.Join(dir, "filter.yaml")
	os.WriteFile(artifact, []byte("\x00asm\x01\x00\x00\x00synthetic"), 0600)
	cmd := exec.Command("python3", "scripts/render_gateway_filter.py", "--wasm-file", artifact, "--output", out, "--enable")
	cmd.Dir = "../.."
	if result, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("renderer: %v %s", err, result)
	}
	b, _ := os.ReadFile(out)
	var resource map[string]any
	if err := yaml.Unmarshal(b, &resource); err != nil {
		t.Fatal(err)
	}
	patches := resource["spec"].(map[string]any)["configPatches"].([]any)
	value := patches[0].(map[string]any)["patch"].(map[string]any)["value"].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)["configuration"].(map[string]any)["value"].(map[string]any)
	message, err := structpb.NewStruct(value)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := proto.Marshal(message)
	c, err := Decode(wire)
	if err != nil || !c.Enabled || c.SchemaVersion != 1 || len(c.Routes) != 1 {
		t.Fatalf("rendered config invalid: %v", err)
	}
}

func TestStructRejectsMalformedOrAmbiguousWire(t *testing.T) {
	entry := func(key string, value []byte) []byte {
		field := protowire.AppendTag(nil, 1, protowire.BytesType)
		field = protowire.AppendString(field, key)
		field = protowire.AppendTag(field, 2, protowire.BytesType)
		field = protowire.AppendBytes(field, value)
		root := protowire.AppendTag(nil, 1, protowire.BytesType)
		return protowire.AppendBytes(root, field)
	}
	number := protowire.AppendTag(nil, 2, protowire.Fixed64Type)
	number = protowire.AppendFixed64(number, math.Float64bits(1.5))
	duplicate := entry("enabled", []byte{32, 1})
	for _, data := range [][]byte{entry("x", number), append(append([]byte{}, duplicate...), duplicate...),
		entry("x", []byte{32, 1, 8, 0}), entry(string([]byte{255}), []byte{32, 1}), entry("x", []byte{26, 255})} {
		if _, err := configurationJSON(data); err == nil {
			t.Fatal("ambiguous/malformed Struct accepted")
		}
	}
	nested := []byte{}
	for i := 0; i < 70; i++ {
		child := protowire.AppendTag(nil, 5, protowire.BytesType)
		child = protowire.AppendBytes(child, nested)
		nested = entry("nested", child)
	}
	if _, err := configurationJSON(nested); err == nil {
		t.Fatal("Struct nesting limit ignored")
	}
}
