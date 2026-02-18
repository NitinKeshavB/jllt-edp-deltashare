# Do We Need to Pass Strategy?

## Short answer

**No.** Strategy is **optional**. Users only need to pass it when they want **DELETE** (teardown). For normal provisioning, strategy can be omitted and defaults to **NEW**; the system already auto-corrects NEW → UPDATE when resources exist.

**Implemented:** Strategy defaults to `NEW` when omitted or empty in YAML/Excel and in the queue consumer. Only **DELETE** must be set explicitly for teardown.

## Current behavior

| Strategy | Who passes it? | What it does |
|----------|----------------|--------------|
| **NEW** | User or default | Ensure recipients, ensure shares, **create** pipelines (and schedules). |
| **UPDATE** | User or auto-detected | Ensure recipients, ensure shares, **only update** existing pipelines (fail if pipeline missing). |
| **DELETE** | User only | Name-only config: unschedule/delete pipelines, delete shares, delete recipients. |

- **Recipients and shares** are already strategy-agnostic: create if missing, update if exists (same for NEW and UPDATE).
- **Pipelines** are the only difference: NEW creates them, UPDATE only updates (and fails if a pipeline does not exist).
- **DELETE** is a different intent (teardown) and uses a different config shape (name-only).

## Recommendation

### 1. Make strategy optional (implemented)

- **Default when omitted:** `NEW` (provision / ensure state).
- **When to pass strategy:**
  - Omit for normal provisioning (create/update). The system will use NEW and may auto-switch to UPDATE when resources already exist.
  - Pass **`strategy: DELETE`** only when you want to tear down (remove recipients, shares, pipelines).

So: **strategy is not required** for YAML/Excel. It is only required when the user wants **DELETE**.

### 2. Do not infer DELETE from config shape

- We could infer DELETE when config is “name-only” (e.g. only recipient and share names). That would make strategy unnecessary for delete too, but:
  - It is ambiguous (e.g. a share with only a name could be “create empty share” or “delete share”).
  - It increases the risk of accidental deletes if someone uploads a minimal file.
- **Recommendation:** Keep explicit **`strategy: DELETE`** for teardown. Do not infer DELETE from the shape of the config.

### 3. Long-term simplification (optional)

- **Unify NEW and UPDATE** into a single “provision” flow:
  - One flow that “ensures” state: recipients, shares, **and pipelines** (create pipeline if missing, update if exists).
  - Then there are only two modes: **provision** (default) and **delete**.
- **Strategy could become:** `provision` | `delete` (or stay NEW/UPDATE/DELETE with NEW and UPDATE behaving the same).
- This would remove the need for strategy for provisioning entirely; only **delete** would need to be explicit.

## Summary

| Question | Answer |
|----------|--------|
| Is strategy required? | **No.** Default is NEW when omitted. |
| When must the user pass strategy? | Only for **DELETE** (teardown). |
| Can we infer DELETE from config? | Not recommended (safety and ambiguity). |
| Can we remove NEW vs UPDATE? | Yes, by making pipelines “ensure” (create or update) like recipients/shares; then strategy is only provision vs delete. |
