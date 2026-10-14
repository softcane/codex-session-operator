# Security

## Reporting a vulnerability

Use **Report a vulnerability** in this repository's **Security → Advisories** tab when private vulnerability reporting is enabled. Include the affected version, reproduction steps, impact and a proposed fix if you have one. Do not include real API keys, bearer tokens, database URLs, private transcripts or other credentials.

If private reporting is unavailable, open an issue asking for a private disclosure channel. Keep exploit details and sensitive data out of the public issue until a maintainer provides one. Report findings against the current release or source revision.

## Deployment boundaries

The operator controls provider resources and may launch executor Pods. Treat its Kubernetes permissions, approved image digests, storage classes, environment profiles and gateway routing policy as administrative configuration. Use a separate restricted executor credential. Keep credentials in Kubernetes Secrets or mounted secret files, and run the gateway over TLS with authenticated, namespace scoped principals.

The gateway database contains conversation inputs, approvals, receipts and history. Restrict access, secure connections, and protect backups accordingly. A gateway principal's namespace permission allows access to conversations in that namespace; caller identity alone does not make a conversation private to one user.

Redact secrets and private content before sharing logs or reports. Do not commit credential files, local kubeconfigs, generated demo credentials or database backups. The no-key demo uses simulated provider responses and generated local credentials; use it as a local playground.

CI runs dependency checksum verification, Go linting, race tests and reachable vulnerability scanning. Release manifests use immutable operator and executor image digests. These checks support review; deployment owners must also maintain their Kubernetes cluster, PostgreSQL, credentials and network policies.
