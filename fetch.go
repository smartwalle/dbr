package dbr

import (
	"context"

	"github.com/smartwalle/dbr/fetch"
)

func (c *Client) Fetch(ctx context.Context, key string, fn func(context.Context) ([]byte, error), opts ...fetch.Option) (value []byte, err error) {
	return fetch.Do(ctx, c, key, fn, opts...)
}
