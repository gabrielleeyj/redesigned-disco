package bill

import (
	"context"
	"time"

	"encore.dev/beta/errs"
	"encore.dev/rlog"
)

type RefreshCurrenciesResponse struct {
	CurrencyCount int       `json:"currencyCount"`
	RefreshedAt   time.Time `json:"refreshedAt"`
}

//encore:api auth method=POST path=/admin/currencies/refresh tag:mutating
//
// RefreshCurrencies forces an immediate reload of the currency
// registry from the DB. Useful after INSERT-ing a new currency so
// the change is visible without waiting for the TTL (1h).
//
// TODO production: gate this on an admin role / scope. Today the
// auth stub only asserts an account identity — any authenticated
// caller can refresh. That's safe (the refresh is read-only and the
// source of truth is the DB) but is not the right
// production posture. Wire role-based access control when the auth
// handler is replaced (see auth.go TODO).
func (s *Service) RefreshCurrencies(ctx context.Context) (*RefreshCurrenciesResponse, error) {
	count, err := refreshCurrencies(ctx)
	if err != nil {
		rlog.Error("currency registry refresh failed", "err", err)
		return nil, &errs.Error{
			Code:    errs.Internal,
			Message: "failed to refresh currency registry",
		}
	}
	return &RefreshCurrenciesResponse{
		CurrencyCount: count,
		RefreshedAt:   time.Now().UTC(),
	}, nil
}
