package exchange

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"sleipnir/internal/telemetry"
)

// BinanceConnector implements the ExchangeConnector interface for Binance Spot.
type BinanceConnector struct {
	apiKey     string
	apiSecret  string
	restURL    string
	wsURL      string
	httpClient *http.Client
	logger     *slog.Logger

	mu        sync.Mutex
}

// NewBinanceConnector creates a new BinanceConnector.
func NewBinanceConnector(apiKey, apiSecret, restURL, wsURL string, logger *slog.Logger) *BinanceConnector {
	if logger == nil {
		logger = slog.Default()
	}
	return &BinanceConnector{
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		restURL:    strings.TrimSuffix(restURL, "/"),
		wsURL:      strings.TrimSuffix(wsURL, "/"),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		logger:     logger.With("module", "binance_connector"),
	}
}

// TranslateToExchange translates standard format (e.g. BTC-USD) to Binance format (e.g. BTCUSDT).
func TranslateToExchange(instrument string) string {
	s := strings.ToUpper(strings.ReplaceAll(instrument, "-", ""))
	if strings.HasSuffix(s, "USD") {
		s = s + "T" // Map USD terms to USDT stablecoin for spot testnet
	}
	return s
}

// TranslateToDownstream translates Binance format (e.g. BTCUSDT) to standard format (e.g. BTC-USD).
func TranslateToDownstream(symbol string) string {
	if strings.HasSuffix(symbol, "USDT") {
		return strings.TrimSuffix(symbol, "USDT") + "-USD"
	}
	if strings.HasSuffix(symbol, "BUSD") {
		return strings.TrimSuffix(symbol, "BUSD") + "-USD"
	}
	return symbol
}

// sign generates the HMAC-SHA256 signature for a query string.
func (bc *BinanceConnector) sign(queryString string) string {
	mac := hmac.New(sha256.New, []byte(bc.apiSecret))
	mac.Write([]byte(queryString))
	return hex.EncodeToString(mac.Sum(nil))
}

// newSignedRequest creates an authenticated HTTP request.
func (bc *BinanceConnector) newSignedRequest(method, path string, params url.Values) (*http.Request, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")

	queryString := params.Encode()
	signature := bc.sign(queryString)
	fullURL := fmt.Sprintf("%s%s?%s&signature=%s", bc.restURL, path, queryString, signature)

	req, err := http.NewRequest(method, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	req.Header.Set("X-MBX-APIKEY", bc.apiKey)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// SubmitOrder submits an order to Binance Spot Testnet.
func (bc *BinanceConnector) SubmitOrder(ctx context.Context, order Order) (ExecutionFill, error) {
	bc.logger.Info("Submitting order to Binance", "orderID", order.OrderID, "instrument", order.Instrument, "qty", order.Quantity, "side", order.Side)

	start := time.Now()
	var finalErr error
	defer func() {
		statusLabel := "success"
		if finalErr != nil {
			statusLabel = "error"
		}
		telemetry.OrderLatency.WithLabelValues("submit", order.Instrument, statusLabel).Observe(time.Since(start).Seconds())
	}()

	params := url.Values{}
	params.Set("symbol", TranslateToExchange(order.Instrument))
	params.Set("side", string(order.Side))
	params.Set("type", string(order.Type))
	params.Set("quantity", strconv.FormatFloat(order.Quantity, 'f', 8, 64))
	params.Set("newClientOrderId", order.OrderID)

	if order.Type == TypeLimit {
		params.Set("price", strconv.FormatFloat(order.Price, 'f', 2, 64))
		params.Set("timeInForce", "GTC")
	}

	req, err := bc.newSignedRequest(http.MethodPost, "/api/v3/order", params)
	if err != nil {
		finalErr = err
		return ExecutionFill{}, fmt.Errorf("failed to build order request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		finalErr = err
		return ExecutionFill{}, fmt.Errorf("failed to send order request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		finalErr = fmt.Errorf("exchange returned non-ok status %d", resp.StatusCode)
		return ExecutionFill{}, fmt.Errorf("exchange returned non-ok status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var binanceResp struct {
		Symbol              string `json:"symbol"`
		OrderID             int64  `json:"orderId"`
		ClientOrderID       string `json:"clientOrderId"`
		TransactTime        int64  `json:"transactTime"`
		Price               string `json:"price"`
		OrigQty             string `json:"origQty"`
		ExecutedQty         string `json:"executedQty"`
		CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
		Status              string `json:"status"`
	}

	if err := json.Unmarshal(bodyBytes, &binanceResp); err != nil {
		finalErr = err
		return ExecutionFill{}, fmt.Errorf("failed to parse order response: %w", err)
	}

	// Calculate filled price
	executedQty, _ := strconv.ParseFloat(binanceResp.ExecutedQty, 64)
	cummulativeQuoteQty, _ := strconv.ParseFloat(binanceResp.CummulativeQuoteQty, 64)
	fillPrice := 0.0
	if executedQty > 0 {
		fillPrice = cummulativeQuoteQty / executedQty
	} else if order.Type == TypeLimit {
		fillPrice, _ = strconv.ParseFloat(binanceResp.Price, 64)
	}

	return ExecutionFill{
		OrderID:         binanceResp.ClientOrderID,
		Instrument:      TranslateToDownstream(binanceResp.Symbol),
		Side:            order.Side,
		Quantity:        executedQty,
		FillPrice:       fillPrice,
		TransactionCost: 0.0, // Binance API Spot REST doesn't return fees directly in order endpoint
		Timestamp:       time.UnixMilli(binanceResp.TransactTime),
	}, nil
}

// CancelOrder cancels an open order on Binance Spot.
func (bc *BinanceConnector) CancelOrder(ctx context.Context, orderID string, instrument string) error {
	bc.logger.Info("Cancelling order on Binance", "orderID", orderID, "instrument", instrument)

	start := time.Now()
	var finalErr error
	defer func() {
		statusLabel := "success"
		if finalErr != nil {
			statusLabel = "error"
		}
		telemetry.OrderLatency.WithLabelValues("cancel", instrument, statusLabel).Observe(time.Since(start).Seconds())
	}()

	params := url.Values{}
	params.Set("symbol", TranslateToExchange(instrument))
	params.Set("origClientOrderId", orderID)

	req, err := bc.newSignedRequest(http.MethodDelete, "/api/v3/order", params)
	if err != nil {
		finalErr = err
		return fmt.Errorf("failed to build cancel request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		finalErr = err
		return fmt.Errorf("failed to send cancel request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		finalErr = fmt.Errorf("cancel returned non-ok status %d", resp.StatusCode)
		return fmt.Errorf("cancel returned non-ok status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// GetOrderState queries the live status of an order on Binance.
func (bc *BinanceConnector) GetOrderState(ctx context.Context, orderID string, instrument string) (OrderState, float64, float64, error) {
	bc.logger.Debug("Querying order state on Binance", "orderID", orderID, "instrument", instrument)

	start := time.Now()
	var finalErr error
	defer func() {
		statusLabel := "success"
		if finalErr != nil {
			statusLabel = "error"
		}
		telemetry.OrderLatency.WithLabelValues("get_state", instrument, statusLabel).Observe(time.Since(start).Seconds())
	}()

	params := url.Values{}
	params.Set("symbol", TranslateToExchange(instrument))
	params.Set("origClientOrderId", orderID)

	req, err := bc.newSignedRequest(http.MethodGet, "/api/v3/order", params)
	if err != nil {
		finalErr = err
		return StatePending, 0, 0, fmt.Errorf("failed to build get-order request: %w", err)
	}
	req = req.WithContext(ctx)

	resp, err := bc.httpClient.Do(req)
	if err != nil {
		finalErr = err
		return StatePending, 0, 0, fmt.Errorf("failed to send get-order request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		finalErr = fmt.Errorf("get order state returned non-ok status %d", resp.StatusCode)
		return StatePending, 0, 0, fmt.Errorf("get order state returned non-ok status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var binanceResp struct {
		Status              string `json:"status"`
		ExecutedQty         string `json:"executedQty"`
		CummulativeQuoteQty string `json:"cummulativeQuoteQty"`
		Price               string `json:"price"`
	}
	if err := json.Unmarshal(bodyBytes, &binanceResp); err != nil {
		finalErr = err
		return StatePending, 0, 0, fmt.Errorf("failed to parse status payload: %w", err)
	}

	executedQty, _ := strconv.ParseFloat(binanceResp.ExecutedQty, 64)
	cummulativeQuoteQty, _ := strconv.ParseFloat(binanceResp.CummulativeQuoteQty, 64)
	fillPrice := 0.0
	if executedQty > 0 {
		fillPrice = cummulativeQuoteQty / executedQty
	} else {
		fillPrice, _ = strconv.ParseFloat(binanceResp.Price, 64)
	}

	return mapBinanceStatus(binanceResp.Status), executedQty, fillPrice, nil
}

// StartUserStream opens the WebSocket feed and publishes live fills back to the channel.
func (bc *BinanceConnector) StartUserStream(ctx context.Context, fillChan chan<- ExecutionFill) error {
	bc.logger.Info("Starting Binance User Data WebSocket stream via WebSocket API...")

	// Connect and start reader loop with automatic recovery
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			bc.logger.Info("Connecting to Binance WS API address", "url", bc.wsURL)

			conn, _, err := websocket.DefaultDialer.DialContext(ctx, bc.wsURL, nil)
			if err != nil {
				bc.logger.Error("Failed to dial Binance WS API, retrying in 5 seconds", "error", err)
				time.Sleep(5 * time.Second)
				continue
			}

			bc.logger.Info("Binance User Data WS connected! Subscribing to user stream...")

			// Construct the signed subscription request
			timestamp := time.Now().UnixMilli()
			queryString := fmt.Sprintf("apiKey=%s&timestamp=%d", bc.apiKey, timestamp)
			signature := bc.sign(queryString)

			subscribeReq := map[string]interface{}{
				"id":     "sleipnir-sub-1",
				"method": "userDataStream.subscribe.signature",
				"params": map[string]interface{}{
					"apiKey":    bc.apiKey,
					"timestamp": timestamp,
					"signature": signature,
				},
			}

			if err := conn.WriteJSON(subscribeReq); err != nil {
				bc.logger.Error("Failed to write subscription request, retrying in 5 seconds", "error", err)
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			bc.logger.Info("Sent userDataStream.subscribe.signature request successfully.")

			// Message reading loop
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					bc.logger.Error("Websocket read failure, reconnecting in 5 seconds", "error", err)
					conn.Close()
					time.Sleep(5 * time.Second)
					break
				}

				var payload map[string]interface{}
				if err := json.Unmarshal(msg, &payload); err != nil {
					bc.logger.Error("Failed to parse websocket message", "error", err)
					continue
				}

				// Check if this is a response to our subscribe request or other requests
				if id, exists := payload["id"]; exists {
					bc.logger.Debug("Received WS-API response", "id", id, "raw", string(msg))
					if errMsg, ok := payload["error"].(map[string]interface{}); ok {
						bc.logger.Error("WS-API returned subscription error", "error", errMsg)
					} else {
						bc.logger.Info("WS-API successfully subscribed to user data stream!")
					}
					continue
				}

				eventType, exists := payload["e"].(string)
				if !exists {
					continue
				}

				if eventType == "executionReport" {
					bc.logger.Debug("Received Binance executionReport", "raw", string(msg))

					// Extract relevant attributes safely
					clientOrderID, _ := payload["c"].(string)
					symbol, _ := payload["s"].(string)
					sideStr, _ := payload["S"].(string)
					execType, _ := payload["x"].(string)
					orderStatus, _ := payload["X"].(string)

					lastFilledQtyStr, _ := payload["l"].(string)
					lastPriceStr, _ := payload["L"].(string)
					commissionStr, _ := payload["n"].(string) // or fee amount

					transactTimeFloat, ok := payload["T"].(float64)
					var transTime int64
					if ok {
						transTime = int64(transactTimeFloat)
					} else {
						transTime = time.Now().UnixMilli()
					}

					// We broadcast trades (execution fill events)
					if execType == "TRADE" || orderStatus == "FILLED" || orderStatus == "PARTIALLY_FILLED" {
						qty, _ := strconv.ParseFloat(lastFilledQtyStr, 64)
						price, _ := strconv.ParseFloat(lastPriceStr, 64)
						commission, _ := strconv.ParseFloat(commissionStr, 64)

						// Only notify downstream if there is actual filled quantity on this event
						if qty > 0 {
							fill := ExecutionFill{
								OrderID:         clientOrderID,
								Instrument:      TranslateToDownstream(symbol),
								Side:            OrderSide(sideStr),
								Quantity:        qty,
								FillPrice:       price,
								TransactionCost: commission,
								Timestamp:       time.UnixMilli(transTime),
							}

							bc.logger.Info("Discovered live execution fill", "orderID", clientOrderID, "instrument", fill.Instrument, "qty", qty, "price", price)
							select {
							case fillChan <- fill:
							case <-ctx.Done():
								conn.Close()
								return
							}
						}
					}
				}
			}
		}
	}()

	return nil
}

func mapBinanceStatus(status string) OrderState {
	switch status {
	case "NEW":
		return StateSubmitted
	case "PARTIALLY_FILLED":
		return StatePartiallyFilled
	case "FILLED":
		return StateFilled
	case "CANCELED":
		return StateCanceled
	case "REJECTED":
		return StateRejected
	case "EXPIRED":
		return StateExpired
	default:
		return StatePending
	}
}
