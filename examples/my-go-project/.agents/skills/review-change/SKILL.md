---
name: review-change
description: Review a my-go-project change for correctness, scope, concurrency and regression coverage.
---

Read the complete diff and relevant callers. Verify request identity behavior,
concurrent access and error handling. Confirm that the regression fails before the
repair and make verify passes afterwards. Check for unrelated edits, credentials,
generated files or weakened assertions. Report actionable findings with evidence;
otherwise report the checked scope and any unverified boundary.
