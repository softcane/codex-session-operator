---
name: fix-bug
description: Investigate, test, repair and verify a reported defect in my-go-project.
---

Read AGENTS.md and the registered golang-how-to skill. Run make bootstrap. Create a
local task branch when editing. Reproduce the reported behavior and add a regression
test that fails on the original code. Repair the smallest relevant code path, run
the focused regression, then make verify. Use review-change before finishing.
Save meaningful failure/success logs under .artifacts/ and report exact commands,
exit codes and limitations. Commit or publish only as authorized in the task.
Git push and PR creation require explicit task authorization and provisioned credentials.
