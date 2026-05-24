package bill

import (
	"context"
	"errors"
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
// SSO, or signed service-to-service tokens. The header form is a
// stub. With this stub the auth handler validates the asserted ID
// against the local clients table (which is itself a stub for an
// external account / identity service). A production deployment
// would replace BOTH layers: the header with a real credential, and
// the local clients lookup with a call to the account service.
type AuthParams struct {
	AccountID string `header:"X-Account-Id"`
}

// AuthData is the per-request identity the rest of the service reads
// via auth.Data(). Carries the client's status so mutating endpoints
// can reject SUSPENDED accounts without re-querying.
type AuthData struct {
	AccountID string
	Status    ClientStatus
	Name      string
}

// AuthHandler resolves the caller's identity:
//   - Missing/empty header → Unauthenticated
//   - Unknown account → Unauthenticated (do NOT differentiate from
//     missing header; otherwise a probe can enumerate valid IDs)
//   - Known account → returns identity. Status (ACTIVE / SUSPENDED)
//     is carried on AuthData so mutating endpoints can enforce.
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

	client, err := lookupClient(ctx, accountID)
	if err != nil {
		if errors.Is(err, ErrClientNotFound) {
			return "", nil, &errs.Error{
				Code:    errs.Unauthenticated,
				Message: "unknown account",
			}
		}
		return "", nil, &errs.Error{
			Code:    errs.Unavailable,
			Message: "failed to resolve caller identity",
		}
	}

	return auth.UID(accountID), &AuthData{
		AccountID: accountID,
		Status:    client.Status,
		Name:      client.Name,
	}, nil
}

// callerAccountID returns the asserted account ID for the current
// request. Empty when no auth context is set (test paths that bypass
// the handler).
func callerAccountID(ctx context.Context) string {
	data, _ := auth.Data().(*AuthData)
	if data == nil {
		return ""
	}
	return data.AccountID
}

// assertActiveCaller rejects SUSPENDED clients on mutating endpoints.
// Reads stay open so a frozen account can still see its balance
// during dispute resolution. Returns PermissionDenied with a generic
// message — we don't leak whether suspension is per-account or
// global to discourage social-engineering attempts.
func assertActiveCaller(ctx context.Context) error {
	data, _ := auth.Data().(*AuthData)
	if data == nil {
		// Defensive — auth handler should have rejected before
		// reaching here. Treat absent identity as Unauthenticated,
		// not Internal, so the response is meaningful.
		return &errs.Error{
			Code:    errs.Unauthenticated,
			Message: "missing caller identity",
		}
	}
	if data.Status != ClientStatusActive {
		return &errs.Error{
			Code:    errs.PermissionDenied,
			Message: "account not permitted to perform this operation",
		}
	}
	return nil
}
