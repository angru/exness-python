// Standalone public consumer. The Python harness builds this in a temporary module.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	exness "github.com/angru/exness-go"
)

type signingVector struct {
	APIKey     string `json:"api_key"`
	PrivateKey string `json:"private_key_hex"`
}

type tickResult struct {
	Instrument *string  `json:"instrument"`
	Bid        *float64 `json:"bid"`
	Ask        *float64 `json:"ask"`
}

type tickTranscript struct {
	Accounts []string    `json:"accounts"`
	Tick     *tickResult `json:"tick"`
	Error    string      `json:"error"`
}

type tradingTranscript struct {
	Events  []map[string]any `json:"events"`
	Outcome string           `json:"outcome"`
}

type writeOutcome struct {
	Error          string `json:"error"`
	Method         string `json:"method"`
	Path           string `json:"path"`
	AccountID      string `json:"account_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

func run(origin, certPath, vectorPath, scenario string) error {
	cert, err := os.ReadFile(certPath)
	if err != nil {
		return errors.New("read loopback certificate")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		return errors.New("trust loopback certificate")
	}
	data, err := os.ReadFile(vectorPath)
	if err != nil {
		return errors.New("read signing fixture")
	}
	var vector signingVector
	if json.Unmarshal(data, &vector) != nil {
		return errors.New("decode signing fixture")
	}
	credentials, err := exness.NewCredentials(vector.APIKey, vector.PrivateKey)
	if err != nil {
		return errors.New("initialize synthetic credentials")
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	}
	defer transport.CloseIdleConnections()
	client, err := exness.NewClient(exness.Config{
		Credentials:  credentials,
		AccountID:    "123456",
		DiscoveryURL: origin,
		Timeout:      3 * time.Second,
		Transport:    transport,
	})
	if err != nil {
		return errors.New("initialize SDK client")
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	switch scenario {
	case "rest_ticks":
		return runTicks(ctx, client)
	case "trading_recovery":
		return runTrading(ctx, client, 1)
	case "trading_missing_snapshot":
		// A protocol error must stop recovery while a second retry is available.
		return runTrading(ctx, client, 2)
	case "lost_ack":
		return runLostAck(ctx, client)
	default:
		return errors.New("unknown scenario")
	}
}

func runLostAck(ctx context.Context, client *exness.Client) error {
	volume, err := exness.NewDecimal("0.01")
	if err != nil {
		return errors.New("initialize synthetic volume")
	}
	request := exness.OpenPositionRequest{
		Instrument: "EURUSD",
		Side:       exness.TradeBuy,
		Volume:     volume,
	}
	ack, err := client.OpenPosition(ctx, request, "transport-lost-ack-0001")
	var unknown *exness.UnknownOutcomeError
	if ack != nil || !errors.As(err, &unknown) {
		return errors.New("expected UnknownOutcomeError after losing the acknowledgement")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]writeOutcome{
		"outcome": {
			Error:          "UnknownOutcomeError",
			Method:         unknown.Method,
			Path:           unknown.Path,
			AccountID:      unknown.AccountID,
			IdempotencyKey: unknown.IdempotencyKey,
		},
	})
}

func runTicks(ctx context.Context, client *exness.Client) error {
	var report tickTranscript
	for range 2 {
		account, err := client.GetAccount(ctx)
		if err != nil || account == nil {
			return errors.New("read account")
		}
		report.Accounts = append(report.Accounts, account.AccountID)
	}
	options := exness.StreamOptions{OpenTimeout: 3 * time.Second}
	for event, err := range client.StreamTicks(ctx, []string{"EURUSD"}, options) {
		if err != nil {
			var protocol *exness.ProtocolError
			if report.Tick == nil || !errors.As(err, &protocol) {
				return errors.New("unexpected tick stream error")
			}
			report.Error = "ProtocolError"
			break
		}
		if report.Tick != nil || event.Tick == nil {
			return errors.New("unexpected tick stream event")
		}
		report.Tick = &tickResult{
			Instrument: event.Tick.Instrument,
			Bid:        event.Tick.Bid,
			Ask:        event.Tick.Ask,
		}
	}
	if report.Error == "" {
		return errors.New("expected a protocol error after the first tick")
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func runTrading(ctx context.Context, client *exness.Client, reconnects int) error {
	options := exness.StreamOptions{
		OpenTimeout:    3 * time.Second,
		MaxReconnects:  reconnects,
		ReconnectDelay: 10 * time.Millisecond,
	}
	report := tradingTranscript{Events: []map[string]any{}, Outcome: "normal_close"}
	for event, err := range client.StreamTrading(ctx, options) {
		if report.Outcome == "ProtocolError" {
			return errors.New("unexpected trading stream item after protocol error")
		}
		if err != nil {
			var protocol *exness.ProtocolError
			if !errors.As(err, &protocol) || event.Snapshot != nil || event.Transaction != nil || event.Gap != nil {
				return errors.New("unexpected trading stream error")
			}
			report.Outcome = "ProtocolError"
			continue
		}
		observation := tradingObservation(event)
		if observation == nil {
			return errors.New("unexpected trading stream event")
		}
		report.Events = append(report.Events, observation)
	}
	if ctx.Err() != nil {
		return errors.New("trading scenario timed out")
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func tradingObservation(event exness.TradingStreamEvent) map[string]any {
	switch {
	case event.Snapshot != nil && event.Transaction == nil && event.Gap == nil:
		return map[string]any{
			"kind":      "snapshot",
			"balance":   event.Snapshot.AccountState.Balance,
			"positions": len(event.Snapshot.Positions),
		}
	case event.Transaction != nil && event.Snapshot == nil && event.Gap == nil:
		transaction := event.Transaction
		var account map[string]any
		if state := transaction.Payload.AccountState; state != nil {
			account = map[string]any{"balance": state.Balance, "leverage": state.Leverage}
		}
		return map[string]any{
			"kind":          "transaction",
			"operation_id":  transaction.OperationID,
			"status":        transaction.Status,
			"positions":     len(transaction.Payload.Positions),
			"deals":         len(transaction.Payload.Deals),
			"account_state": account,
		}
	case event.Gap != nil && event.Snapshot == nil && event.Transaction == nil:
		return map[string]any{
			"kind":              "gap",
			"reconnect_attempt": event.Gap.ReconnectAttempt,
			"close_code":        event.Gap.CloseCode,
		}
	default:
		return nil
	}
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: transport-driver ORIGIN CERTIFICATE SIGNING_VECTOR SCENARIO")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3], os.Args[4]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
