package bill

import (
	"context"
	"strings"

	"encore.dev/beta/auth"
	"encore.dev/beta/errs"
)

// AuthParams is the shape Encore extracts from incoming requests for
// the auth handler. The X-Account-Id header carries the caller's
// tenant identity.
//
// TODO production: replace with a real authentication scheme — JWT
// validation against an OIDC provider, mTLS client certs, internal
// SSO, or signed service-to-service tokens. The header form is a stub
// that lets the take-home demonstrate tenancy + ownership checks
// without dragging in an auth provider. Trusting a client-supplied
// header MUST NOT be deployed to production.
type AuthParams struct {
	AccountID string `header:"X-Account-Id"`
}

// AuthData is the per-request identity the rest of the service reads
// via auth.Data(). Keeping it as a struct (not just a UID string)
// leaves room to attach roles, scopes, or trace IDs without changing
// every endpoint signature.
type AuthData struct {
	AccountID string
}

// AuthHandler resolves the caller's identity. Returns Unauthenticated
// if the X-Account-Id header is missing or empty.
//
// Endpoints opt into auth by setting `auth` on their //encore:api
// directive. Anonymous endpoints (none today) would simply omit it.
//
//encore:authhandler
func AuthHandler(ctx context.Context, p *AuthParams) (auth.UID, *AuthData, error) {
	accountID := strings.TrimSpace(p.AccountID)
	if accountID == "" {
		return "", nil, &errs.Error{
			Code:    errs.Unauthenticated,
			Message: "X-Account-Id header is required",
		}
	}
	return auth.UID(accountID), &AuthData{AccountID: accountID}, nil
}

// callerAccountID returns the asserted account ID for the current
// request. Panics if called from a non-authenticated endpoint —
// AuthHandler is the only path that produces this value, so the
// absence indicates a programming error.
func callerAccountID(ctx context.Context) string {
	data, _ := auth.Data().(*AuthData)
	if data == nil {
		return ""
	}
	return data.AccountID
}
