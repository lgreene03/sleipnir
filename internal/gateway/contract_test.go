package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sleipnir/internal/exchange"
)

// TestHuginnIntentV1ContractDecodes guards against silent cross-repo wire
// drift. The recorded blob at testdata/huginn_intent_v1.json was captured
// from huginn's kafka.Producer.PublishIntent JSON encoding. If huginn ever
// renames a field on its GatewayOrder type (no tooling enforces this), this
// test fails — catching what JSON's "ignore unknown fields" default would
// otherwise let slip through silently.
//
// See docs/CONTRACTS.md "Field-rename hazard" for the discussion.
//
// To re-record: temporarily print msg.Value in
// `huginn/internal/kafka/producer.go:PublishIntent` and copy the output here.
func TestHuginnIntentV1ContractDecodes(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile(filepath.Join("testdata", "huginn_intent_v1.json"))
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}

	var intent exchange.Order
	if err := json.Unmarshal(data, &intent); err != nil {
		t.Fatalf("decoding the recorded huginn intent blob failed: %v\nblob: %s",
			err, string(data))
	}

	// Field-level assertions. Each one corresponds to a contract field
	// huginn promises to send. If a rename or type change silently lands,
	// one of these flips false and pinpoints the broken field.
	if intent.OrderID != "huginn-1716123456789" {
		t.Errorf("order_id = %q, want huginn-1716123456789", intent.OrderID)
	}
	if intent.Instrument != "BTC-USD" {
		t.Errorf("instrument = %q, want BTC-USD", intent.Instrument)
	}
	if intent.Side != exchange.SideBuy {
		t.Errorf("side = %q, want BUY", intent.Side)
	}
	if intent.Quantity != 0.001 {
		t.Errorf("quantity = %v, want 0.001", intent.Quantity)
	}
	if intent.Type != exchange.TypeMarket {
		t.Errorf("order_type = %q, want MARKET", intent.Type)
	}
}

// TestExecutionFillV1ContractRoundtrip is the reverse direction: encode a
// sleipnir ExecutionFill and assert every field huginn's GatewayFill cares
// about is present in the JSON output. Catches a sleipnir-side rename that
// would silently leave huginn reading zero values.
func TestExecutionFillV1ContractRoundtrip(t *testing.T) {
	t.Parallel()
	fill := exchange.ExecutionFill{
		OrderID:         "ord-1",
		ExecutionID:     "ord-1-ws-9876",
		Instrument:      "BTC-USD",
		Side:            exchange.SideBuy,
		OrderStatus:     exchange.StateFilled,
		Quantity:        0.001,
		FillPrice:       43520.10,
		TransactionCost: 0.04352,
	}
	data, err := json.Marshal(fill)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Field probe: decode into a generic map and assert every expected key
	// is present. Catches snake_case→camelCase drift and field renames.
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("re-decode: %v", err)
	}
	wantKeys := []string{
		"order_id", "execution_id", "instrument", "side",
		"order_status", "quantity", "fill_price", "transaction_cost", "timestamp",
	}
	for _, k := range wantKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("expected key %q in ExecutionFill JSON, got %s", k, string(data))
		}
	}
}
