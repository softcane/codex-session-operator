# Agent Sandbox CRD test fixtures

These unmodified CRD schemas come from `k8s/crds/` in
[kubernetes-sigs/agent-sandbox v1.0.7](https://github.com/kubernetes-sigs/agent-sandbox/tree/v1.0.7/k8s/crds).
They are licensed under Apache-2.0; see [LICENSE](LICENSE).

Controller envtest installs these schemas alongside this project's CRDs. The
upstream controller is not started by envtest; deployed lifecycle tests install
the pinned upstream release separately in disposable Kind clusters.

To upgrade, replace all four files from the same upstream release, update the
Go module pin and isolated installer version, then run the full test suite.
Do not regenerate these schemas with this project's controller-gen.

SHA-256 checksums:

```text
a8659f321adcdd42a6d74fb941c7199fab78c7449c6d85f1adacf75266369c07  agents.x-k8s.io_sandboxes.yaml
a56399ca6ce8708910a8f6c1ff2fecd355100b0a6807b7f5c955868bb3963d18  extensions.agents.x-k8s.io_sandboxclaims.yaml
90f3ace51db1d30ae5d1de78ed3aafc80395ce66f4c617cbc73d5d8532f1f999  extensions.agents.x-k8s.io_sandboxtemplates.yaml
3f2a0788cba4f081e61cc10f4d4431826afb0e5e252bcf8a9211401b9dce3ef2  extensions.agents.x-k8s.io_sandboxwarmpools.yaml
```
