package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var erroredUpstream = errors.New("errored upstream")

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

type paymentGatewayGetPaymentsResponseOne struct {
	Amount int    `json:"amount"`
	Status string `json:"status"`
}

// Every attempt for a ride carries the ride ID as its Idempotency-Key, so the
// gateway processes the payment at most once and answers a repeated key with
// the first result. A failed attempt is therefore simply resent: the reference
// path's GET /payments (300 ms at the gateway) and 100 ms back-off kept the
// evaluation transaction open, and the chair busy, for about 400 ms per failure.
func requestPaymentGatewayPostPayment(ctx context.Context, paymentGatewayURL string, token string, idempotencyKey string, param *paymentGatewayPostPaymentRequest) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		switch {
		case res.StatusCode == http.StatusNoContent:
			return nil
		case res.StatusCode == http.StatusBadRequest:
			// The gateway recorded the payment and rejected its amount or token;
			// the reference path also counted it as processed.
			return nil
		default:
			// 409: the same key is still being processed; 5xx: an injected failure,
			// possibly after the payment was recorded. Both are safe to resend.
			lastErr = fmt.Errorf("[POST /payments] unexpected status code (%d)", res.StatusCode)
		}
	}
	return fmt.Errorf("%v: %w", lastErr, erroredUpstream)
}
