package gateway

import (
	"context"
	"io"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type sessionWriter struct {
	client.Client
	creates    int
	fail       error
	lose       bool
	deleteFail bool
	createRace func(client.Object) error
}

func (w *sessionWriter) Create(ctx context.Context, object client.Object, options ...client.CreateOption) error {
	w.creates++
	if w.createRace != nil {
		return w.createRace(object)
	}
	if w.fail != nil {
		return w.fail
	}
	object.SetUID(types.UID("gateway-session-uid"))
	if err := w.Client.Create(ctx, object, options...); err != nil {
		return err
	}
	if w.lose {
		w.lose = false
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (w *sessionWriter) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if w.deleteFail {
		return io.ErrUnexpectedEOF
	}
	return w.Client.Delete(ctx, object, options...)
}
