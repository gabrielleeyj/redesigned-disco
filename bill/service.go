package bill

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"encore.dev/rlog"
	"encore.dev/storage/sqldb"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const (
	TaskQueue           = "bill-task-queue"
	defaultTemporalHost = "localhost:7233"
	temporalHostEnvVar  = "TEMPORAL_HOSTPORT"
)

var db = sqldb.NewDatabase("bill", sqldb.DatabaseConfig{
	Migrations: "./migrations",
})

//encore:service
type Service struct {
	temporalClient client.Client
	temporalWorker worker.Worker
}

// initService is called by the Encore runtime when the service starts up.
// The function is referenced by the //encore:service annotation above.
//
//nolint:unused // invoked by Encore framework via //encore:service
func initService() (*Service, error) {
	c, err := client.Dial(client.Options{
		HostPort: temporalHostPort(),
	})
	if err != nil {
		return nil, err
	}

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(BillingWorkflow)
	w.RegisterActivity(PersistBillActivity)

	err = w.Start()
	if err != nil {
		c.Close()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loaded, err := loadCurrencies(ctx)
	if err != nil {
		w.Stop()
		c.Close()
		return nil, fmt.Errorf("load currency registry: %w", err)
	}
	setCurrencies(loaded)
	rlog.Info("currency registry loaded", "count", len(loaded))

	return &Service{
		temporalClient: c,
		temporalWorker: w,
	}, nil
}

//nolint:unused // called by initService, which is invoked by Encore
func loadCurrencies(ctx context.Context) (map[Currency]CurrencyMeta, error) {
	rows, err := db.Query(ctx, `
		SELECT code, name, numeric_code, minor_unit
		FROM currencies`)
	if err != nil {
		return nil, fmt.Errorf("query currencies: %w", err)
	}
	defer rows.Close()

	out := make(map[Currency]CurrencyMeta)
	for rows.Next() {
		var (
			meta        CurrencyMeta
			numericCode sql.NullInt32
		)
		if err := rows.Scan(&meta.Code, &meta.Name, &numericCode, &meta.Decimals); err != nil {
			return nil, fmt.Errorf("scan currency: %w", err)
		}
		if numericCode.Valid {
			meta.NumericCode = int(numericCode.Int32)
		}
		out[Currency(meta.Code)] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate currencies: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("currencies table is empty — has the seed migration run?")
	}
	return out, nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.temporalWorker.Stop()
	s.temporalClient.Close()
	return nil
}

//nolint:unused // called by initService, which is invoked by Encore
func temporalHostPort() string {
	if h := os.Getenv(temporalHostEnvVar); h != "" {
		return h
	}
	return defaultTemporalHost
}
