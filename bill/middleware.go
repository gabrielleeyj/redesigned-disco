package bill

import (
	"encore.dev/middleware"
)

// RequireActiveCaller rejects SUSPENDED callers on mutating endpoints.
// Targets the "mutating" tag rather than every API so reads remain
// available to suspended accounts (they can still inspect their own
// balance during dispute resolution).
//
// The auth handler establishes identity and loads ClientStatus onto
// AuthData; this middleware enforces the policy that requires ACTIVE
// status for state changes. Splitting identity (handler) from
// authorization (middleware) keeps each layer single-purpose.
//
//encore:middleware target=tag:mutating
func (s *Service) RequireActiveCaller(req middleware.Request, next middleware.Next) middleware.Response {
	if err := assertActiveCaller(req.Context()); err != nil {
		return middleware.Response{Err: err}
	}
	return next(req)
}
