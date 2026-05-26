package gateway

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"go.yaml.in/yaml/v2"

	"sleipnir/internal/exchange"
)

// InstrumentLimits is the per-instrument risk cap loaded from risk.yaml.
// Zero on any field means "no cap" — useful for letting an instrument
// through with no max while still constraining sibling instruments.
type InstrumentLimits struct {
	MaxQty      float64 `yaml:"max_qty"`
	MaxNotional float64 `yaml:"max_notional"`
	MinQty      float64 `yaml:"min_qty"`
}

// RiskPolicy captures the post-Phase-6 risk rules. A nil RiskPolicy means
// "use the legacy hardcoded BTC/ETH behaviour" — see NewLegacyRiskPolicy.
//
// Loaded from a YAML file at the path the operator points RISK_CONFIG_PATH at:
//
//	default_max_qty: 0.0          # 0 = deny instruments not in the map
//	default_max_notional: 0.0
//	instruments:
//	  BTC-USD:
//	    max_qty: 0.1
//	    max_notional: 100000.0
//	  ETH-USD:
//	    max_qty: 2.0
//	    max_notional: 100000.0
//
// Addresses audit finding C3 (Critical): the pre-Phase-6 risk check let any
// non-BTC/ETH instrument through with no size cap.
type RiskPolicy struct {
	// DefaultMaxQty applies when the instrument isn't in Instruments. The
	// zero value (0) means "deny unknown instruments" — the safe default.
	DefaultMaxQty      float64                     `yaml:"default_max_qty"`
	DefaultMaxNotional float64                     `yaml:"default_max_notional"`
	Instruments        map[string]InstrumentLimits `yaml:"instruments"`
}

// LoadRiskPolicy reads risk.yaml from the given path. Empty path returns
// (nil, nil) — caller falls back to the legacy policy.
func LoadRiskPolicy(path string) (*RiskPolicy, error) {
	if path == "" {
		return nil, nil
	}
	// Path is operator-controlled via RISK_CONFIG_PATH env var; not a user
	// input source. gosec's G304 generic "tainted-path" lint doesn't apply.
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("risk config %s: %w", path, err)
	}
	var p RiskPolicy
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("risk config %s: %w", path, err)
	}
	// Normalize instrument keys to uppercase so "btc-usd" and "BTC-USD"
	// don't bypass each other's caps. (Audit C3 follow-on.)
	norm := make(map[string]InstrumentLimits, len(p.Instruments))
	for k, v := range p.Instruments {
		norm[strings.ToUpper(k)] = v
	}
	p.Instruments = norm
	return &p, nil
}

// NewLegacyRiskPolicy returns a RiskPolicy that mirrors the pre-Phase-6
// hardcoded BTC/ETH-only checks. Used when no RISK_CONFIG_PATH is set so
// existing deployments don't break. Instruments outside this map will go
// through with NO cap — which is the bug audit C3 documents. The Phase 6
// roadmap entry calls this out; ops should land a risk.yaml ASAP.
func NewLegacyRiskPolicy(maxBTC, maxETH float64) *RiskPolicy {
	return &RiskPolicy{
		// DefaultMaxQty = 0 ⇒ unknown instruments unconstrained (legacy bug).
		// We DELIBERATELY preserve this here; switching to deny-by-default on
		// the legacy path would break operators who haven't yet authored a
		// risk.yaml. The yaml-driven path's zero-default IS deny-by-default.
		DefaultMaxQty:      0,
		DefaultMaxNotional: 0,
		Instruments: map[string]InstrumentLimits{
			"BTC-USD": {MaxQty: maxBTC},
			"BTCUSDT": {MaxQty: maxBTC},
			"ETH-USD": {MaxQty: maxETH},
			"ETHUSDT": {MaxQty: maxETH},
		},
	}
}

// ErrRiskRejected wraps a rejection reason for typed error matching.
var ErrRiskRejected = errors.New("risk rejected")

// CheckIntent applies the policy to an intent. Returns a stable reason string
// suitable for telemetry labels and an error when the intent is rejected.
// Behaviour matrix:
//
//	instrument in policy.Instruments → use that InstrumentLimits
//	otherwise                        → use Default* fields
//	zero limit                       → "no cap" on the legacy path,
//	                                   "deny" on the yaml-driven path
//	                                   when Default* are non-zero (anything
//	                                   above the default rejects)
//
// The legacy bug is preserved deliberately for backwards compat; see
// NewLegacyRiskPolicy. Operators should set RISK_CONFIG_PATH to a real yaml.
func (p *RiskPolicy) CheckIntent(intent exchange.Order) (bool, string) {
	if p == nil {
		return true, ""
	}
	limits, known := p.Instruments[strings.ToUpper(intent.Instrument)]
	if !known {
		// Unknown instrument: fall through to defaults. If both defaults
		// are zero, the legacy behaviour ("anything goes") is preserved.
		limits = InstrumentLimits{
			MaxQty:      p.DefaultMaxQty,
			MaxNotional: p.DefaultMaxNotional,
		}
	}
	if limits.MinQty > 0 && intent.Quantity < limits.MinQty {
		return false, "qty_below_minimum"
	}
	if limits.MaxQty > 0 && intent.Quantity > limits.MaxQty {
		return false, "qty_limit_exceeded"
	}
	if limits.MaxNotional > 0 && intent.Price > 0 {
		notional := intent.Quantity * intent.Price
		if notional > limits.MaxNotional {
			return false, "notional_limit_exceeded"
		}
	}
	return true, ""
}

// Halt is an in-memory kill switch. Operators can flip it via the /admin/halt
// HTTP endpoint; the gateway consults IsHalted before submitting any intent
// to the exchange. Survives the lifetime of a single process — a restart
// resets it to false, which is intentional: operators should know if the
// process restarted.
type Halt struct {
	mu     sync.RWMutex
	active bool
	reason string
}

// NewHalt builds a non-halted Halt.
func NewHalt() *Halt { return &Halt{} }

// Set enables the halt with the given operator-supplied reason.
func (h *Halt) Set(reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = true
	if reason == "" {
		reason = "operator_halt"
	}
	h.reason = reason
}

// Clear lifts the halt.
func (h *Halt) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.active = false
	h.reason = ""
}

// IsHalted reports whether trading is currently halted.
func (h *Halt) IsHalted() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.active
}

// Reason returns the operator-supplied reason for the current halt, or "".
func (h *Halt) Reason() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.reason
}

// Validation rejection reasons. Stable strings — used as Prometheus label
// values on `sleipnir_risk_rejections_total{reason=...}` and as the
// `reject_reason` span attribute. Keep cardinality bounded.
const (
	ReasonOrderIDEmpty       = "orderid_empty"
	ReasonOrderIDTooLong     = "orderid_too_long"
	ReasonOrderIDInvalidChar = "orderid_invalid_char"
	ReasonOrderIDDuplicate   = "orderid_duplicate"
)

// MaxOrderIDLen bounds the OrderID a producer may submit. Binance Spot's
// `newClientOrderId` is documented at 36 chars; we accept up to 64 to absorb
// producer-side prefixes (e.g. huginn's `huginn-live-order-<ns>-<n>` form
// runs ~39 chars). Anything longer is almost certainly malformed and would
// be rejected at the exchange anyway — fail fast here, don't burn a rate
// limiter token.
const MaxOrderIDLen = 64

// ValidateOrderID enforces a safe character class and length on incoming
// intent OrderIDs before they propagate into the signed Binance request or
// the in-memory tracker. Closes audit finding H4: an attacker with publish
// rights to the intents topic could otherwise inject Binance-reserved
// characters (causing post-rate-limit rejection — a cheap DoS / fee burn)
// or collide on an existing OrderID to overwrite tracker state.
//
// Accepted set is the conservative `[A-Za-z0-9_-]` — broad enough for every
// real producer in this stack, narrow enough to keep URL-encoding and HMAC
// signing total. A duplicate-active-OrderID check lives separately in the
// gateway dispatch path, since it needs the tracker.
func ValidateOrderID(orderID string) error {
	if orderID == "" {
		return errors.New(ReasonOrderIDEmpty)
	}
	if len(orderID) > MaxOrderIDLen {
		return errors.New(ReasonOrderIDTooLong)
	}
	for _, r := range orderID {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9':
		case r == '_', r == '-':
		default:
			return errors.New(ReasonOrderIDInvalidChar)
		}
	}
	return nil
}
