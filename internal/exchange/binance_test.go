package exchange

import (
	"log/slog"
	"os"
	"testing"
)

func TestSymbolTranslation(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"BTC-USD", "BTCUSDT"},
		{"ETH-USD", "ETHUSDT"},
		{"BTC-USDT", "BTCUSDT"},
		{"ETH-BTC", "ETHBTC"},
	}

	for _, tt := range tests {
		result := TranslateToExchange(tt.input)
		if result != tt.expected {
			t.Errorf("TranslateToExchange(%q) = %q; expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestSymbolTranslationDownstream(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"BTCUSDT", "BTC-USD"},
		{"ETHUSDT", "ETH-USD"},
		{"BTCBUSD", "BTC-USD"},
	}

	for _, tt := range tests {
		result := TranslateToDownstream(tt.input)
		if result != tt.expected {
			t.Errorf("TranslateToDownstream(%q) = %q; expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestBinanceSignature(t *testing.T) {
	fakeAPIKey := "fake_key_12345"
	fakeSecret := "NhqPtmd3uWYwDxT1MVb7OkprMD8RttZ7099CqgZgeih9WUMgIPwT6dfgjh56ULww"
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	connector := NewBinanceConnector(fakeAPIKey, fakeSecret, "https://testnet.binance.vision", "wss://testnet.binance.vision/ws", logger)

	queryString := "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
	expectedSig := "524b916c594748aa7134c2c6181da3b70a5ddee37c97641b9ad61e8872f7bbbf"

	sig := connector.sign(queryString)
	if sig != expectedSig {
		t.Errorf("sign(%q) = %q; expected %q", queryString, sig, expectedSig)
	}
}
