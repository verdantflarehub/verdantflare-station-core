package identity

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// WithManager is for authenticated service delegation, never public client claims.
// Shared row locks serialize permission revocation against the bounded effect.
// Accepted background operations rely on current membership, not a stored bearer.
func (s *Service) WithManager(ctx context.Context, user, org string, effect func(context.Context) error) error {
	return s.withDelegatedMember(ctx, user, org, true, effect)
}

// WithMember is reserved for service-side access resolution after application
// ACL checks; ordinary instance editors need not be Station administrators.
func (s *Service) WithMember(ctx context.Context, user, org string, effect func(context.Context) error) error {
	return s.withDelegatedMember(ctx, user, org, false, effect)
}

func (s *Service) withDelegatedMember(ctx context.Context, user, org string, manager bool, effect func(context.Context) error) error {
	for _, value := range []string{user, org} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return Invalid
		}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var permitted bool
	err = tx.QueryRow(ctx, `SELECT u.status='active' AND o.status='active' AND m.active AND (NOT $3 OR m.role='admin') AND (u.locked_until IS NULL OR u.locked_until<=now()) FROM station.users u JOIN station.memberships m USING(user_id) JOIN station.organizations o USING(organization_id) WHERE u.user_id=$1 AND o.organization_id=$2 FOR SHARE OF u,o,m`, user, org, manager).Scan(&permitted)
	if errors.Is(err, pgx.ErrNoRows) {
		return Denied
	}
	if err != nil {
		return err
	}
	if !permitted {
		return Denied
	}
	if err = effect(ctx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
