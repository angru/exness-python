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
	"strings"
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

type streamTranscript struct {
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

const writeKey = "transport-lost-ack-0001"
const acknowledgedOpenKey = "transport-acknowledged-open-0001"

func run(origin, certPath, vectorPath, scenario, streamOrigin string) error {
	if scenario == "lost_ack_recovery" && streamOrigin == "" {
		return errors.New("missing recovery stream fixture origin")
	}
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
	config := exness.Config{
		Credentials:  credentials,
		AccountID:    "123456",
		DiscoveryURL: origin,
		Timeout:      3 * time.Second,
		Transport:    transport,
	}
	client, err := exness.NewClient(config)
	if err != nil {
		return errors.New("initialize SDK client")
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	switch scenario {
	case "rest_ticks":
		return runTicks(ctx, client)
	case "orders_history_repeated_cursor":
		return runOrdersHistory(ctx, client)
	case "trading_recovery":
		return runTrading(ctx, client, 1)
	case "trading_missing_snapshot":
		// A protocol error must stop recovery while a second retry is available.
		return runTrading(ctx, client, 2)
	case "hmr_recovery":
		return runHMR(ctx, client, 1)
	case "hmr_missing_snapshot":
		return runHMR(ctx, client, 2)
	case "instrument_recovery":
		return runInstruments(ctx, client)
	case "acknowledged_open", "acknowledged_open_rate_limited":
		return runAcknowledgedOpen(ctx, client, scenario == "acknowledged_open_rate_limited")
	case "lost_ack", "lost_ack_recovery":
		unknown, err := submitLostAck(ctx, client)
		if err != nil {
			return err
		}
		report := map[string]any{"outcome": writeOutcome{
			Error:          "UnknownOutcomeError",
			Method:         unknown.Method,
			Path:           unknown.Path,
			AccountID:      unknown.AccountID,
			IdempotencyKey: unknown.IdempotencyKey,
		}}
		if scenario == "lost_ack_recovery" {
			reference := exness.OperationReference{ClientRequestID: unknown.IdempotencyKey}
			config.DiscoveryURL = streamOrigin
			streamClient, err := exness.NewClient(config)
			if err != nil {
				return errors.New("initialize recovery stream client")
			}
			defer streamClient.Close()
			events, err := runLostAckRecovery(ctx, streamClient, reference)
			if err != nil {
				return err
			}
			report["events"] = events
		}
		return json.NewEncoder(os.Stdout).Encode(report)
	default:
		return errors.New("unknown scenario")
	}
}

func runOrdersHistory(ctx context.Context, client *exness.Client) error {
	from := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	query := exness.OrderHistoryQuery{
		HistoryQuery: exness.HistoryQuery{
			From:       &from,
			Instrument: "EURUSD",
			Limit:      7,
		},
		OrderState: exness.OrderFilled,
	}
	orderIDs := []string{}
	outcome := ""
	for item, err := range client.IterOrdersHistory(ctx, query) {
		if outcome != "" {
			return errors.New("unexpected order history item after protocol error")
		}
		if err == nil {
			orderIDs = append(orderIDs, item.Order.OrderID)
			continue
		}
		var protocol *exness.ProtocolError
		if !errors.As(err, &protocol) || item.Order.OrderID != "" {
			return errors.New("unexpected order history result")
		}
		message := strings.ToLower(protocol.Error())
		if !strings.Contains(message, "repeat") || !strings.Contains(message, "cursor") {
			return errors.New("unexpected order history protocol error")
		}
		outcome = "ProtocolError: repeated continuation cursor"
	}
	if outcome == "" {
		return errors.New("expected a repeated continuation cursor error")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"order_ids": orderIDs, "outcome": outcome})
}

func submitOpenPosition(ctx context.Context, client *exness.Client, key string) (*exness.AckResponse, error) {
	volume, err := exness.NewDecimal("0.01")
	if err != nil {
		return nil, errors.New("initialize synthetic volume")
	}
	request := exness.OpenPositionRequest{
		Instrument: "EURUSD",
		Side:       exness.TradeBuy,
		Volume:     volume,
	}
	return client.OpenPosition(ctx, request, key)
}

func submitLostAck(ctx context.Context, client *exness.Client) (*exness.UnknownOutcomeError, error) {
	ack, err := submitOpenPosition(ctx, client, writeKey)
	var unknown *exness.UnknownOutcomeError
	if ack != nil || !errors.As(err, &unknown) {
		return nil, errors.New("expected UnknownOutcomeError after losing the acknowledgement")
	}
	if unknown.IdempotencyKey != writeKey {
		return nil, errors.New("lost acknowledgement did not preserve the caller key")
	}
	return unknown, nil
}

func runAcknowledgedOpen(ctx context.Context, client *exness.Client, rateLimited bool) error {
	ack, err := submitOpenPosition(ctx, client, acknowledgedOpenKey)
	if err != nil || ack == nil {
		return errors.New("expected an open-position acknowledgement")
	}
	reference, err := exness.OperationReferenceFromAck(*ack, acknowledgedOpenKey)
	if err != nil {
		return errors.New("build operation reference from acknowledgement")
	}
	timeout := 3 * time.Second
	if rateLimited {
		// A retry that honors Retry-After must have time to reach the fixture.
		timeout = 10 * time.Second
	}
	result, err := client.WaitOperation(ctx, reference, exness.OperationWaitOptions{
		Timeout:      timeout,
		PollInterval: 10 * time.Millisecond,
	})
	var outcome map[string]any
	if rateLimited {
		var apiError *exness.APIError
		if result != nil || !errors.As(err, &apiError) {
			return errors.New("expected APIError while waiting for open-position confirmation")
		}
		outcome = map[string]any{
			"error":       "APIError",
			"status_code": apiError.StatusCode,
			"retry_after": apiError.RetryAfter,
		}
	} else {
		if err != nil || result == nil {
			return errors.New("wait for open-position confirmation")
		}
		outcome = map[string]any{
			"status":            result.Status,
			"operation_id":      result.OperationID,
			"client_request_id": result.ClientRequestID,
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"ack": map[string]any{
			"status":            ack.Status,
			"operation_id":      ack.OperationID,
			"client_request_id": ack.ClientRequestID,
		},
		"outcome": outcome,
	})
}

func runLostAckRecovery(ctx context.Context, client *exness.Client, reference exness.OperationReference) ([]map[string]any, error) {
	options := exness.StreamOptions{OpenTimeout: 3 * time.Second}
	var events []map[string]any
	var selected map[string]any
	for event, err := range client.StreamTrading(ctx, options) {
		if err != nil {
			return nil, errors.New("unexpected recovery stream error")
		}
		observation := tradingObservation(event)
		if observation == nil {
			return nil, errors.New("unexpected trading stream event")
		}
		if transaction := event.Transaction; transaction != nil {
			matches, err := reference.MatchesEvent(*transaction)
			if err != nil {
				return nil, errors.New("correlate recovery transaction")
			}
			switch {
			case !matches:
				observation["decision"] = "ignored"
			case transaction.Status == nil:
				observation["decision"] = "matched_without_status"
			default:
				if selected != nil {
					return nil, errors.New("extra matching terminal transaction")
				}
				selected = map[string]any{"operation_id": transaction.OperationID, "status": transaction.Status}
				observation["decision"] = "selected"
			}
		}
		observation["selected"] = selected
		events = append(events, observation)
	}
	if ctx.Err() != nil {
		return nil, errors.New("recovery stream timed out")
	}
	if selected == nil {
		return nil, errors.New("expected a matching terminal transaction")
	}
	return events, nil
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
	report := streamTranscript{Events: []map[string]any{}, Outcome: "normal_close"}
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

func runHMR(ctx context.Context, client *exness.Client, reconnects int) error {
	options := exness.ServerStreamOptions{
		OpenTimeout:    3 * time.Second,
		MaxReconnects:  reconnects,
		ReconnectDelay: 10 * time.Millisecond,
	}
	report := streamTranscript{Events: []map[string]any{}, Outcome: "normal_close"}
	for event, err := range client.StreamHMR(ctx, []string{"EURUSD"}, options) {
		if report.Outcome == "ProtocolError" {
			return errors.New("unexpected HMR stream item after protocol error")
		}
		if err != nil {
			var protocol *exness.ProtocolError
			if !errors.As(err, &protocol) || event.Snapshot != nil || event.Update != nil || event.Gap != nil {
				return errors.New("unexpected HMR stream error")
			}
			message := strings.ToLower(protocol.Error())
			if !strings.Contains(message, "hmr update") ||
				!strings.Contains(message, "before") || !strings.Contains(message, "snapshot") {
				return errors.New("unexpected HMR protocol error")
			}
			report.Outcome = "ProtocolError"
			continue
		}
		switch {
		case event.Snapshot != nil && event.Update == nil && event.Gap == nil:
			report.Events = append(report.Events, map[string]any{
				"kind":       "snapshot",
				"event_type": event.Snapshot.EventType,
				"periods":    event.Snapshot.Periods,
			})
		case event.Update != nil && event.Snapshot == nil && event.Gap == nil:
			report.Events = append(report.Events, map[string]any{
				"kind":               "update",
				"event_type":         event.Update.EventType,
				"upserted_periods":   event.Update.UpsertedPeriods,
				"removed_period_ids": event.Update.RemovedPeriodIDs,
			})
		case event.Gap != nil && event.Snapshot == nil && event.Update == nil:
			report.Events = append(report.Events, map[string]any{
				"kind":              "gap",
				"reconnect_attempt": event.Gap.ReconnectAttempt,
				"close_code":        event.Gap.CloseCode,
			})
		default:
			return errors.New("unexpected HMR stream event")
		}
	}
	if ctx.Err() != nil {
		return errors.New("HMR scenario timed out")
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func runInstruments(ctx context.Context, client *exness.Client) error {
	options := exness.ServerStreamOptions{
		OpenTimeout:    3 * time.Second,
		MaxReconnects:  2, // A normal close must stop while a reconnect is still available.
		ReconnectDelay: 10 * time.Millisecond,
	}
	report := streamTranscript{Events: []map[string]any{}, Outcome: "normal_close"}
	for event, err := range client.StreamInstruments(ctx, []string{"EURUSD"}, options) {
		if err != nil {
			return errors.New("unexpected instrument stream error")
		}
		switch {
		case event.Instrument != nil && event.Gap == nil:
			instrument := event.Instrument
			payload := instrument.Payload
			report.Events = append(report.Events, map[string]any{
				"kind":         "instrument",
				"event_type":   instrument.EventType,
				"action":       instrument.Action,
				"instrument":   payload.Instrument,
				"point_digits": payload.PointDigits,
				"trade_mode":   payload.TradeMode,
				"volume_min":   payload.VolumeMin,
			})
		case event.Gap != nil && event.Instrument == nil:
			report.Events = append(report.Events, map[string]any{
				"kind":              "gap",
				"reconnect_attempt": event.Gap.ReconnectAttempt,
				"close_code":        event.Gap.CloseCode,
			})
		default:
			return errors.New("unexpected instrument stream event")
		}
	}
	if ctx.Err() != nil {
		return errors.New("instrument scenario timed out")
	}
	return json.NewEncoder(os.Stdout).Encode(report)
}

func main() {
	if len(os.Args) != 5 && len(os.Args) != 6 {
		fmt.Fprintln(os.Stderr, "usage: transport-driver ORIGIN CERTIFICATE SIGNING_VECTOR SCENARIO [STREAM_ORIGIN]")
		os.Exit(2)
	}
	var streamOrigin string
	if len(os.Args) == 6 {
		streamOrigin = os.Args[5]
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3], os.Args[4], streamOrigin); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
