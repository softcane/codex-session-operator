# Security

## Reporting a vulnerability

Use **Report a vulnerability** in this repository's **Security → Advisories** tab when private vulnerability reporting is enabled. Include the affected version, reproduction steps, impact and a proposed fix if you have one. Do not include real API keys, bearer tokens, database URLs, private transcripts or other credentials.

If private reporting is unavailable, open an issue asking for a private disclosure channel. Keep exploit details and sensitive data out of the public issue until a maintainer provides one. Report findings against the current release or source revision.

## Deployment boundaries

The controller manages provider identities and Session-owned SandboxClaims. Agent Sandbox creates executor Pods and workspace PVCs from administrator-approved templates and pools. Treat Kubernetes permissions, pool approval, immutable template configuration and gateway exposure as administrative configuration. Use a separate restricted executor credential and keep credentials in Kubernetes Secrets or mounted secret files. Deployment owners provide gateway access control and any required TLS termination; the gateway itself does not authenticate callers or enforce user roles.

The gateway database contains conversation inputs, approvals, receipts and history. Restrict access, secure connections, and protect backups accordingly. All callers who can reach a namespace's gateway routes share access to its conversations. The gateway's service replay subject is not a user identity or isolation boundary.

Redact secrets and private content before sharing logs or reports. Do not commit credential files, local kubeconfigs, generated demo credentials or database backups. The no-key demo uses simulated provider responses and isolated local infrastructure; use it as a local playground.

CI runs dependency checksum verification, Go linting, race tests and reachable vulnerability scanning. Release manifests use immutable operator and executor image digests. These checks support review; deployment owners must also maintain their Kubernetes cluster, PostgreSQL, credentials and network policies.
