---
name: investigate
description: Reproduce and explain a defect in my-go-project without changing application behavior.
---

Read AGENTS.md, the Go skill and architecture documentation. Run make bootstrap.
Trace the report through HTTP handling and the job store. Reproduce with a focused
test or synthetic request. Capture commands, output and the relevant code path.
Separate observed facts from hypotheses. Save findings under .artifacts/. Report a
blocked investigation when the evidence is insufficient; do not invent a cause.
