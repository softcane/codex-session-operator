"""Administrator-owned upstream Sandbox configuration used by isolated helpers."""

import copy

VERSION = "v1.0.7"
RELEASE = "https://github.com/kubernetes-sigs/agent-sandbox/releases/download/" + VERSION


def install(checks):
    """Install the pinned upstream operator separately from this project's installer."""
    checks.kubectl("apply", "--server-side", "-f", RELEASE + "/sandbox-with-extensions.yaml")
    checks.kubectl("wait", "--for=condition=Established", "crd/sandboxes.agents.x-k8s.io",
                   "crd/sandboxclaims.extensions.agents.x-k8s.io",
                   "crd/sandboxtemplates.extensions.agents.x-k8s.io",
                   "crd/sandboxwarmpools.extensions.agents.x-k8s.io", "--timeout=90s")
    checks.kubectl("rollout", "status", "deployment/agent-sandbox-controller", "-n", "agent-sandbox-system", "--timeout=180s")


def resources(name, namespace, image, architecture, repository_url, commit, *, storage_class="standard",
              private=False, fixture=False, mount_credentials=False, tmp_size="128Mi"):
    """Return standard template and zero-spare pool; no Session or provider calls."""
    security = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
    limits = {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}
    executor = {"name": "executor", "image": image, "imagePullPolicy": "IfNotPresent", "workingDir": "/workspace",
        "command": ["codex", "exec-server"],
        "args": ["--remote", "$(CODEX_REMOTE_URL)", "--environment-id", "$(CODEX_ENVIRONMENT_ID)"],
        "env": [{"name": "CODEX_API_KEY", "valueFrom": {"secretKeyRef": {"name": "codex-executor", "key": "CODEX_API_KEY"}}}],
        "securityContext": copy.deepcopy(security), "resources": copy.deepcopy(limits),
        "volumeMounts": [{"name": "workspace", "mountPath": "/workspace", "subPath": "project"},
                         {"name": "home", "mountPath": "/home/node"}, {"name": "tmp", "mountPath": "/tmp"}]}
    preparation = {"name": "repository-prepare", "image": image, "imagePullPolicy": "IfNotPresent",
        "command": ["/workspace-bootstrap"], "args": ["--storage-root", "/workspace-storage", "--session-uid", "$(SESSION_UID)",
                                                     "--url", repository_url, "--commit", commit],
        "env": [{"name": "TMPDIR", "value": "/tmp"}], "securityContext": copy.deepcopy(security),
        "resources": copy.deepcopy(limits), "volumeMounts": [{"name": "workspace", "mountPath": "/workspace-storage"},
                                                             {"name": "tmp", "mountPath": "/tmp"}]}
    pod = {"automountServiceAccountToken": False, "restartPolicy": "Always", "terminationGracePeriodSeconds": 10,
        "nodeSelector": {"kubernetes.io/arch": architecture},
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "initContainers": [preparation], "containers": [executor],
        "volumes": [{"name": "home", "emptyDir": {"sizeLimit": "128Mi"}}, {"name": "tmp", "emptyDir": {"sizeLimit": tmp_size}}]}
    if private:
        pod["volumes"].append({"name": "repository-credentials", "secret": {"secretName": "codex-repository", "defaultMode": 288,
                                                                                  "items": [{"key": "token", "path": "token"}]}})
        mount = {"name": "repository-credentials", "mountPath": "/repository-credentials", "readOnly": True}
        preparation["volumeMounts"].append(mount)
        preparation["args"] += ["--credential-directory", "/repository-credentials"]
        if mount_credentials:
            executor["volumeMounts"].append(copy.deepcopy(mount))
            executor["env"] += [{"name": "GIT_ASKPASS", "value": "/workspace-bootstrap"}, {"name": "GIT_TERMINAL_PROMPT", "value": "0"},
                {"name": "WORKSPACE_BOOTSTRAP_CREDENTIAL_DIRECTORY", "value": "/repository-credentials"},
                {"name": "WORKSPACE_BOOTSTRAP_REPOSITORY_URL", "value": repository_url}]
    spec = {"envVarsInjectionPolicy": "Allowed", "service": False, "podTemplate": {"spec": pod}}
    spec["volumeClaimTemplates"] = [{"metadata": {"name": "workspace"}, "spec": {"accessModes": ["ReadWriteOnce"],
        "storageClassName": storage_class, "resources": {"requests": {"storage": "1Gi"}}}}]
    if fixture:
        # Fixture services are inside this isolated cluster, unlike production endpoints.
        spec["networkPolicyManagement"] = "Unmanaged"
    template = {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxTemplate",
                "metadata": {"name": name, "namespace": namespace, "labels": {"agents.softcane.io/managed": "true"}}, "spec": spec}
    pool = {"apiVersion": "extensions.agents.x-k8s.io/v1beta1", "kind": "SandboxWarmPool",
            "metadata": {"name": name, "namespace": namespace, "labels": {"agents.softcane.io/managed": "true"}},
            "spec": {"replicas": 0, "sandboxTemplateRef": {"name": name}}}
    return template, pool
