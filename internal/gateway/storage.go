package gateway

import "context"

// Storage returns independent records and follows caller cancellation. Update
// is an atomic read/modify/write; callbacks must contain no network calls.
// An unavailable commit is never acceptance. An uncertain COMMIT may finish a
// bounded, detached outcome check before reporting whether it persisted.
type Storage interface {
	Get(context.Context, string) (Record, error)
	Records(context.Context) ([]Record, error)
	GetInNamespace(context.Context, string, string) (Record, error)
	RecordsInNamespace(context.Context, string) ([]Record, error)
	Update(context.Context, string, func(*Record) error) error
	Purge(context.Context, string) error
	Close() error
}

// DeliveryStorage elects one delivery worker, independent of HTTP writers.
// The returned view fences reads and mutations by a durable ownership epoch.
type DeliveryStorage interface {
	Storage
	DeliveryAllowed(context.Context) error
	ClaimWorker(context.Context) (Storage, func(), error)
}

// IndexedStorage bounds worker scans by active reservations and provides compact
// listings and indexed replay lookup independently of retained transcript size.
type IndexedStorage interface {
	ActiveRecords(context.Context) ([]Record, error)
	RecordSummaries(context.Context, string, string) ([]Record, error)
	LookupWorkEvent(context.Context, string, string, string) (Record, WorkEvent, bool, error)
	LookupIssueDelivery(context.Context, string, string, string) (Record, IssueDelivery, bool, error)
}

// ConversationEntry holds compact metadata for one public conversation attempt.
type ConversationEntry struct {
	Record  Record
	Attempt WorkAttempt
}

// ConversationStorage resolves a public conversation identity without scanning
// retained transcripts. Listing returns up to limit+1 entries in identity order;
// the extra entry lets callers determine whether another page exists.
type ConversationStorage interface {
	LookupConversation(context.Context, string, string) (Record, WorkAttempt, error)
	ListConversationAttempts(context.Context, string, string, int) ([]ConversationEntry, error)
}

func (g *Gateway) activeRecords(ctx context.Context) ([]Record, error) {
	if store, ok := g.store.(IndexedStorage); ok {
		return store.ActiveRecords(ctx)
	}
	return g.store.Records(ctx)
}

func (g *Gateway) recordSummaries(ctx context.Context, namespace, kind string) ([]Record, error) {
	if store, ok := g.store.(IndexedStorage); ok {
		return store.RecordSummaries(ctx, namespace, kind)
	}
	return g.store.RecordsInNamespace(ctx, namespace)
}

func (g *Gateway) deliveryEnabled(ctx context.Context) error {
	if store, ok := g.store.(DeliveryStorage); ok {
		return store.DeliveryAllowed(ctx)
	}
	return nil
}
