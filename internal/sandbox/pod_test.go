package sandbox

import (
	"strings"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodConfigurationBoundaries(t *testing.T) {
	s := &agentsv1alpha1.Session{ObjectMeta: metav1.ObjectMeta{UID: "test-session"},
		Status: agentsv1alpha1.SessionStatus{ExecutorImage: "example.com/executor@sha256:" + strings.Repeat("a", 64)}}
	for _, workspace := range []string{"relative", "/", "/usr/bin", "/tmp/data", "/home/node", "/workspace/../usr"} {
		if _, err := Pod(s, workspace, "test"); err == nil {
			t.Errorf("Accepted unsafe workspace %s", workspace)
		}
	}
	for _, image := range []string{"", "executor:latest", "executor@sha256:bad", "executor@sha256:" + strings.Repeat("z", 64)} {
		if err := ValidateImage(image); err == nil {
			t.Errorf("Accepted unpinned image %s", image)
		}
	}
	if _, err := Pod(s, defaultWorkspaceDirectory, "test"); err != nil {
		t.Fatal(err)
	}
}
