package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/openaiapi"
	"github.com/softcane/codex-session-operator/internal/sandbox"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type executorRuntimeProvider struct {
	SessionProvider
	state   openaiapi.HistoryEnvironment
	err     error
	onRead  func()
	reads   int
	bounded bool
}

func (p *executorRuntimeProvider) ConnectionState(ctx context.Context, _ string) (openaiapi.HistoryEnvironment, error) {
	p.reads++
	deadline, ok := ctx.Deadline()
	p.bounded = ok && time.Until(deadline) > 0 && time.Until(deadline) <= 20*time.Second
	if p.onRead != nil {
		p.onRead()
	}
	return p.state, p.err
}

func TestIdleSandboxReplacementRecoversOnlyCurrentRuntimeEvidence(t *testing.T) {
	for _, change := range []string{_noneMode, "stream revision", "lost subscription", "pod replaced", "workspace replaced", "provider pending", "provider error"} {
		t.Run(change, func(t *testing.T) {
			r, s, observer := preparationFixture(t)
			for range 5 {
				preparationStep(t, r, s)
			}
			pod := preparationPod(t, r, s)
			healthyPreparationPod(t, r, pod)
			observer.state.Connected = true
			observer.state.Sequence = 1
			preparationStep(t, r, s)
			if !meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
				t.Fatal("initial executor not ready")
			}
			if err := r.Delete(t.Context(), pod, client.GracePeriodSeconds(0)); err != nil {
				t.Fatal(err)
			}
			preparationStep(t, r, s)
			replacement := preparationPod(t, r, s)
			healthyPreparationPod(t, r, replacement)
			preparationStep(t, r, s)
			if meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) {
				t.Fatal("replacement reused stale SSE evidence")
			}
			provider := &executorRuntimeProvider{state: openaiapi.HistoryEnvironment{ID: s.Status.EnvironmentID, Type: executorRuntimeSelfHostedType, Status: "connected"}}
			switch change {
			case "stream revision":
				provider.onRead = func() { observer.state.Sequence++ }
			case "lost subscription":
				provider.onRead = func() { observer.state.Subscribed = false }
			case "pod replaced":
				provider.onRead = func() {
					if err := r.Delete(t.Context(), replacement, client.GracePeriodSeconds(0)); err != nil {
						t.Fatal(err)
					}
				}
			case "workspace replaced":
				provider.onRead = func() {
					var proof agentsv1alpha1.Session
					s.DeepCopyInto(&proof)
					proof.Status.WorkspaceClaimUID = _changedIdentityUID
					if err := r.Status().Update(t.Context(), &proof); err != nil {
						t.Fatal(err)
					}
				}
			case "provider pending":
				provider.state.Status = "pending"
			case "provider error":
				provider.err = errors.New("unavailable")
			}
			r.OpenAI = provider
			preparationStep(t, r, s)
			if meta.IsStatusConditionTrue(s.Status.Conditions, readyCondition) != (change == _noneMode) {
				t.Fatal("runtime fallback accepted stale identity or rejected current runtime", change)
			}
			if provider.reads != 1 || !provider.bounded {
				t.Fatal("runtime recovery was not one bounded authoritative read")
			}
			if change == _noneMode && s.Status.WorkspacePreparation.ConfigDigest == "" {
				t.Fatal("runtime blueprint digest absent")
			}
			_ = sandbox.ConfigAnnotation
		})
	}
}
