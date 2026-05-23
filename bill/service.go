package bill

import (
	"context"
	"os"

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

	return &Service{
		temporalClient: c,
		temporalWorker: w,
	}, nil
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
