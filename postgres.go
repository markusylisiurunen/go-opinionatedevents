package opinionatedevents

import (
	"context"
	"strings"
)

func WithTx(ctx context.Context, tx sqlTx) context.Context {
	return context.WithValue(ctx, postgresContextKeyForTx, tx)
}

func withSchema(query string, schema string) string {
	return strings.ReplaceAll(query, ":SCHEMA", schema)
}
