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
	Bid         *float64 `json:"bid"`
	Ask         *float64 `json:"ask"`
}

type transcript struct {
	Accounts []string    `json:"accounts"`
	Tick     *tickResult `json:"tick"`
	Error    string      `json:"error"`
}

func run(origin, certPath, vectorPath string) error {
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

	var report transcript
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

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: transport-driver ORIGIN CERTIFICATE SIGNING_VECTOR")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
