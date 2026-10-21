package sandbox

import (
	"reflect"
	"testing"

	agentsv1alpha1 "github.com/softcane/codex-session-operator/api/v1alpha1"
	"github.com/softcane/codex-session-operator/internal/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	extensionsv1beta1 "sigs.k8s.io/agent-sandbox/extensions/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPoolValidationRejectsUnsafeOrIncompatibleTemplates(t *testing.T) {
	pool, template := testutil.NewSandboxPool("tenant")
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _testWorkspace, DeletionPolicy: WorkspaceClaimRetain}}
	if err := ValidatePool(profile, pool, template); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*agentsv1alpha1.SelfHostedEnvironment, *extensionsv1beta1.SandboxWarmPool, *extensionsv1beta1.SandboxTemplate)
	}{
		{name: "unmanaged template", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			delete(p.Labels, ManagedSetupLabel)
		}},
		{name: "unmanaged pool", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, p *extensionsv1beta1.SandboxWarmPool, _ *extensionsv1beta1.SandboxTemplate) {
			delete(p.Labels, ManagedSetupLabel)
		}},
		{name: "warm pool", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, p *extensionsv1beta1.SandboxWarmPool, _ *extensionsv1beta1.SandboxTemplate) {
			p.Spec.Replicas = new(int32(1))
		}},
		{name: "default warm count", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, p *extensionsv1beta1.SandboxWarmPool, _ *extensionsv1beta1.SandboxTemplate) {
			p.Spec.Replicas = nil
		}},
		{name: "cross namespace", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Namespace = _testOther
		}},
		{name: "missing template UID", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.UID = ""
		}},
		{name: "mutable image", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Image = "executor:latest"
		}},
		{name: "credential override", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.EnvVarsInjectionPolicy = extensionsv1beta1.EnvVarsInjectionPolicyOverrides
		}},
		{name: "predefined Session", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Env = append(p.Spec.PodTemplate.Spec.Containers[0].Env, corev1.EnvVar{Name: "CODEX_SESSION_UID", Value: _testOther})
		}},
		{name: "inline credential", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Env[0] = corev1.EnvVar{Name: ExecutorSecretKey, Value: "private"}
		}},
		{name: "wrong Secret", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Env[0].ValueFrom.SecretKeyRef.Name = _testOther
		}},
		{name: "duplicate credential", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Env = append(p.Spec.PodTemplate.Spec.Containers[0].Env, p.Spec.PodTemplate.Spec.Containers[0].Env[0])
		}},
		{name: "mutable configuration source", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].Env = append(p.Spec.PodTemplate.Spec.Containers[0].Env, corev1.EnvVar{Name: "UNTRACKED", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "dynamic"}, Key: "value"}}})
		}},
		{name: "envFrom", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: ExecutorSecretName}}}}
		}},
		{name: "service account", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.AutomountServiceAccountToken = new(true)
		}},
		{name: "host network", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.HostNetwork = true
		}},
		{name: "root user", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.SecurityContext.RunAsUser = new(int64(0))
		}},
		{name: "unconfined executor", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}
		}},
		{name: "privileged", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Containers[0].SecurityContext.Privileged = new(true)
		}},
		{name: "existing PVC", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.PodTemplate.Spec.Volumes = []corev1.Volume{{Name: "foreign", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "foreign"}}}}
		}},
		{name: "missing storage class", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.VolumeClaimTemplates[0].Spec.StorageClassName = nil
		}},
		{name: "storage clone", mutate: func(_ *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, p *extensionsv1beta1.SandboxTemplate) {
			p.Spec.VolumeClaimTemplates[0].Spec.DataSource = &corev1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "existing"}
		}},
		{name: "workspace mismatch", mutate: func(p *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, _ *extensionsv1beta1.SandboxTemplate) {
			p.Workspace.Directory = "/another"
		}},
		{name: "implicit retention", mutate: func(p *agentsv1alpha1.SelfHostedEnvironment, _ *extensionsv1beta1.SandboxWarmPool, _ *extensionsv1beta1.SandboxTemplate) {
			p.Workspace.DeletionPolicy = ""
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copied, p, c := *profile, pool.DeepCopy(), template.DeepCopy()
			tt.mutate(&copied, p, c)
			if ValidatePool(&copied, p, c) == nil {
				t.Fatal("unsafe or incompatible template accepted")
			}
		})
	}
}

func TestResolvePoolFreezesIdentityAndTemplateChanges(t *testing.T) {
	pool, template := testutil.NewSandboxPool("tenant")
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _testWorkspace, DeletionPolicy: WorkspaceClaimDelete}}
	scheme := runtime.NewScheme()
	if err := extensionsv1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pool, template).Build()
	before, err := ResolvePool(t.Context(), reader, "tenant", profile)
	if err != nil {
		t.Fatal(err)
	}
	if before.TemplateUID != string(template.UID) || before.UID != string(pool.UID) || len(before.ConfigurationDigest) != 64 {
		t.Fatal("missing frozen infrastructure identities")
	}
	var changed extensionsv1beta1.SandboxTemplate
	if err := reader.Get(t.Context(), client.ObjectKeyFromObject(template), &changed); err != nil {
		t.Fatal(err)
	}
	changed.Spec.PodTemplate.Spec.Containers[0].Env = append(changed.Spec.PodTemplate.Spec.Containers[0].Env, corev1.EnvVar{Name: "PROJECT_MODE", Value: "new"})
	if err := reader.Update(t.Context(), &changed); err != nil {
		t.Fatal(err)
	}
	after, err := ResolvePool(t.Context(), reader, "tenant", profile)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(before, after) || before.ConfigurationDigest == after.ConfigurationDigest {
		t.Fatal("template mutation reused frozen configuration")
	}
	if _, err := ResolvePool(t.Context(), reader, "different-namespace", profile); err == nil {
		t.Fatal("cross-namespace pool resolved")
	}
}

func TestPreparationAndSecretSourcesComeFromTemplate(t *testing.T) {
	_, template := testutil.NewSandboxPool("tenant")
	template.Spec.PodTemplate.Spec.Volumes = []corev1.Volume{{Name: "repository-credential", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: RepositorySecretName, Items: []corev1.KeyToPath{{Key: _testToken, Path: _testToken}}}}}}
	if got := SecretNames(template); !reflect.DeepEqual(got, []string{ExecutorSecretName, RepositorySecretName}) {
		t.Fatalf("unexpected Secret identities: %v", got)
	}
	if got := SecretKeys(template)[RepositorySecretName]; !reflect.DeepEqual(got, []string{_testToken}) {
		t.Fatal("repository key was not tracked")
	}
	pod := &corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: RepositoryInitContainerName}}}}
	if InitContainersPrepared(pod) {
		t.Fatal("missing init observation accepted")
	}
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: RepositoryInitContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, FinishedAt: metav1.Now()}}}}
	if !InitContainersPrepared(pod) {
		t.Fatal("successful preparation was not accepted")
	}
	pod.Status.InitContainerStatuses[0].State.Terminated.ExitCode = 1
	if InitContainersPrepared(pod) {
		t.Fatal("failed preparation accepted")
	}
}

func TestExecutorCredentialIsNeverAvailableToPreparation(t *testing.T) {
	pool, template := testutil.NewSandboxPool("tenant")
	profile := &agentsv1alpha1.SelfHostedEnvironment{SandboxPoolRef: agentsv1alpha1.AgentReference{Name: pool.Name}, Workspace: agentsv1alpha1.SelfHostedWorkspace{Directory: _testWorkspace, DeletionPolicy: WorkspaceClaimDelete}}
	init := *template.Spec.PodTemplate.Spec.Containers[0].DeepCopy()
	init.Name = RepositoryInitContainerName
	template.Spec.PodTemplate.Spec.InitContainers = []corev1.Container{init}
	if ValidatePool(profile, pool, template) == nil {
		t.Fatal("preparation inherited restricted executor credential via environment")
	}
	template.Spec.PodTemplate.Spec.InitContainers[0].Env = nil
	template.Spec.PodTemplate.Spec.Volumes = []corev1.Volume{{Name: "executor-token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: ExecutorSecretName}}}}
	template.Spec.PodTemplate.Spec.InitContainers[0].VolumeMounts = append(init.VolumeMounts, corev1.VolumeMount{Name: "executor-token", MountPath: "/credential"})
	if ValidatePool(profile, pool, template) == nil {
		t.Fatal("preparation inherited restricted executor credential via mounted Secret")
	}
	template.Spec.PodTemplate.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: ExecutorSecretName}}}}}}
	if ValidatePool(profile, pool, template) == nil {
		t.Fatal("preparation inherited restricted executor credential via projected Secret")
	}
	template.Spec.PodTemplate.Spec.InitContainers[0].VolumeMounts = init.VolumeMounts
	if err := ValidatePool(profile, pool, template); err != nil {
		t.Fatalf("unmounted executor-only Secret blocked valid preparation: %v", err)
	}
}
