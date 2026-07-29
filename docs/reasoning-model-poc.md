# Reasoning-model POC — spec, plan, adversarial review

Status: **design only, nothing built.** Written 2026-07-29.

Goal: a locally-trained ~4B reasoning model whose improvement over a baseline is
**measurable and defensible to someone deciding whether to fund larger models.**
The deliverable is not the model. The deliverable is a credible number, plus the
apparatus that makes the number mean something.

Everything below assumes verified facts about this machine, not assumptions:

| Fact | Verified |
|---|---|
| RTX 5080, 16 GB, **compute capability 12.0 (Blackwell / sm_120)** | `nvidia-smi` |
| Driver 595.84, CUDA 13.2 | `nvidia-smi` |
| Ollama 0.32.4 running, `gpt-oss:20b` + `qwen3.5:9b` pulled | `ollama list` |
| No torch in the JupyterHub image | `python -c "import torch"` fails |
| thesis-app: pgvector **0.8.5**, `vector(384)`, **fully populated** (17,152 highlights, 4,472 sources) | direct query |
| thesis-app: hierarchical tags — `tags.parent_id`, 1,057 tags, 10,374 highlight links | direct query |
| thesis-app: `tsvector` full-text columns alongside the vectors | schema |
| Corpus ≈ **5.81 M characters** ⇒ roughly 1.45–1.66 M tokens | `sum(length(...))` |
| `sources.extracted_text` is **empty** — only highlights and summaries exist | direct query |
| MinIO has **no buckets yet** — the PDF corpus does not exist | `ls minio/data` |

---

# 1. SPEC

## 1.1 What is being built

Six components. Only the middle two are novel; the rest is plumbing this
platform already knows how to do.

| # | Component | Substance |
|---|---|---|
| A | **Corpus ingest** | thesis-app Postgres → `raw` via dlt sql_database. MinIO PDFs → `raw` via dlt filesystem, once the bucket exists. |
| B | **Split & curation** | A held-out assignment and an inclusion flag per item, stored in the warehouse. The contamination firewall. |
| C | **Retrieval** | Tag prefilter → hybrid (BM25 + vector) → rerank → top-k. Serves a small, precise context. |
| D | **Training** | Unsloth LoRA/QLoRA on a ~4B base → merge → GGUF → Ollama. |
| E | **Evaluation** | Inspect (`inspect_ai`) tasks over a frozen, versioned eval set; Claude as model-graded scorer. |
| F | **Results** | Inspect logs → `raw` → dbt marts → Cube. Same pattern as `airflow_meta`. |

## 1.2 Non-goals

Explicitly out of scope, so nobody builds them by accident:

- Production or multi-user serving. One person, one GPU.
- Frontier scale. 4B is the *point* — it shows deltas without 500B compute.
- Real-time or low-latency inference.
- Replacing the thesis app. This reads from it; it never writes back.

## 1.3 Invariants

These are the assertions that make the number credible. Violating any one of
them invalidates the POC, and most are unrecoverable after the fact.

1. **The held-out split is decided before any training data is generated,**
   stored as a column, and respected by every consumer. No item may serve as
   both training data and eval source.
2. **The eval set is immutable per version.** Corrections create a new version;
   rows are never edited in place. A score is only comparable to another score
   from the same eval version.
3. **The judge is a different model family from the model under test.** Claude
   grades; the local Llama is graded. Never self-grading.
4. **Every eval run records full provenance** — base model, LoRA adapter hash,
   quantization, retrieval config, prompt version, eval-set version, judge model.
   A score without provenance is unattributable and therefore worthless.
5. **The artifact evaluated is the artifact served.** Evaluate the exported,
   quantized GGUF running under Ollama — not the fp16 training-time model.
6. **One variable per comparison.** Changing retrieval and weights together
   produces a number nobody can attribute.

## 1.4 Baseline definition

Pinned now, because "improvement over baseline" is meaningless otherwise. The
baseline is:

> The **same base model**, **no LoRA adapter**, **same retrieval configuration**,
> **same prompt**, **same eval set version**, **same quantization**, served the
> same way.

Anything else — a different base, retrieval-off, a different prompt — is a
*separate* ablation and must be reported as one, not as "the baseline".

---

# 2. PLAN

Ordered by dependency. Phases 0–2 are prerequisites that gate everything; doing
them out of order is how the invariants get violated.

## Phase 0 — Toolchain proof (blocks everything)

Before any design work, prove the GPU stack runs on this card. See adversarial
review R1 — this is the highest-probability failure in the project.

1. Create a Python env with a **Blackwell-capable** PyTorch (CUDA 12.8+ build).
2. Verify `torch.cuda.is_available()` **and** that a real kernel executes —
   a matmul on-device, not just a version string.
3. Install Unsloth; run its smallest example end to end.
4. Only then proceed.

**Exit criterion:** a LoRA step completes on a 4B model without a
`no kernel image is available for execution on the device` error.

## Phase 1 — Ingest and split (blocks training and eval)

1. `thesis_app` dlt source → `raw`. Column **allowlist**, as with `airflow_meta`
   — thesis-app holds user records and sync credentials.
2. dbt staging for highlights, sources, tags, and the tag hierarchy.
3. **Assign the held-out split** — deterministic hash on a stable id, stored as
   a column. Split at the **source** level, not the highlight level (see R2).
4. Curation table keyed by source id: inclusion flag, notes. Editable without
   re-ingest.

**Exit criterion:** every highlight and source carries a split label, and the
split is reproducible from the id alone.

## Phase 2 — Eval set (blocks any claim of improvement)

Design and populate the frozen eval set. **Not implemented here — see §4** for
what it is and what it involves. Build it *before* touching retrieval or
training, so both have a target to move.

**Exit criterion:** a versioned eval set exists, drawn only from held-out
sources, with rubrics a judge can apply.

## Phase 3 — Retrieval

1. Re-embed with a stronger model than the current 384-dim (R5). Land vectors in
   the warehouse (`pgvector` — image swap already staged).
2. Hybrid search: tsvector BM25 + vector, fused.
3. Cross-encoder rerank to final k (6–10).
4. Tag prefilter using the existing hierarchy.
5. **Measure retrieval on its own** — recall@k against the eval set — before
   involving the model at all.

**Exit criterion:** retrieval recall@k is known and is not the binding
constraint.

## Phase 4 — Training

1. Generate training examples from **train-split items only**.
2. Unsloth LoRA/QLoRA on the ~4B base.
3. Merge, export GGUF, load into Ollama.
4. Confirm the served artifact answers sanely before evaluating it.

## Phase 5 — Evaluation harness

1. Inspect task: dataset from the warehouse eval set, solver calling
   `ollama/<model>`, scorer `model_graded_qa` against `anthropic/claude-opus-5`.
2. Run baseline and candidate on the **same eval version**.
3. Ingest Inspect logs → `raw` → dbt → Cube.

## Phase 6 — Report

Charts from Cube: score by eval version, by config, by case category. The
provenance columns are what make the chart defensible.

---

# 3. IS INSPECT THE RIGHT TOOL?

**Yes — it is a good fit, and better than hand-rolling.** Verified against the
docs rather than recalled:

| Requirement | Inspect |
|---|---|
| Local Llama under test | `ollama/<model>`; base URL via `OLLAMA_BASE_URL`, defaults to `http://localhost:11434/v1` |
| Claude as judge | `anthropic/<model>`; built-in `model_graded_qa()` scorer |
| Cases + targets | `Dataset` of samples with input and target/grading guidance |
| Per-sample inspection | Inspect View — a web log viewer |
| Programmatic export | Logs readable as dataframes |

It also structures the parts that are easy to get wrong: separating dataset from
solver from scorer means swapping the judge, or the model, without touching the
cases.

**What it does not give you**, and must be built:

- **Warehouse integration.** Inspect writes log files, not database rows. An
  ingest step (dlt filesystem over the log directory, or a small exporter) is
  needed to get results into `raw` for dbt and Cube. This is the same shape as
  every other source here, so it is cheap — but it is not free.
- **The eval set itself.** Inspect runs cases; it does not author them.
- **Provenance capture.** Inspect logs its own config, but the retrieval config,
  adapter hash, and quantization are yours to record.

**Recommendation:** use Inspect for Phase 5, and treat its log directory as just
another dlt filesystem source. Do not build a bespoke runner.

---

# 4. THE EVAL SET — what it is, what building it involves

*Explanation only; deliberately not implemented.*

A **fixed, versioned collection of test cases with known-good answers**, used to
measure whether the model improved. It is not RAG and needs no embeddings. Its
defining property is the opposite of the corpus: a RAG corpus should be fresh;
an eval set must be **frozen**, or scores across runs are not comparable.

What building it involves:

| Piece | Substance |
|---|---|
| **Storage** | Table: case id, input/question, expected answer *or* rubric, category tags, `version`. Append-only. |
| **Case sourcing** | The hard part, and it is judgment rather than engineering. Curating 50–200 genuinely discriminating cases from held-out sources. A hundred good cases beats a thousand generated ones. |
| **Runner** | Per case: assemble context, call the model, record the answer *plus* the exact config that produced it. |
| **Judge** | Claude scoring against the rubric. Note `temperature` is **removed on Opus 5** (400 error) — consistency comes from `output_config.format` with a JSON schema, not `temperature=0`. |
| **Results table** | One row per (case, run), feeding dbt and Cube like everything else. |
| **Human spot-checks** | Periodic audit of the judge, or you are measuring the judge. |

Calibrate difficulty to 4B. Cases a 4B cannot do under any condition measure
nothing; cases it always gets right measure nothing. The discriminating band is
narrow at this scale and is where the cases must sit.

---

# 5. ADVERSARIAL REVIEW

Assume the plan above is wrong. Where does it break?

## R1 — The Blackwell toolchain will fight you *(highest probability)*

This card is **sm_120**. Much of the ecosystem — older torch, xformers, triton,
bitsandbytes wheels — was built for sm_90 and below and fails at runtime with
`no kernel image is available for execution on the device`, often *after* a
successful install and a truthful `torch.cuda.is_available() == True`.

**Why it is the top risk:** it is invisible until a kernel actually launches, it
looks like a code bug, and every layer (torch, Unsloth, bitsandbytes, the
quantizer) can independently be the culprit.

**Mitigation:** Phase 0 exists solely for this. Prove a real kernel executes
before designing anything. Pin the exact working versions immediately and record
them, because a later `pip install -U` can silently revert you.

## R2 — Contamination will invalidate the POC, quietly

If a highlight is in training data and its source also produces an eval case,
the model gets credit for memorization. Reviewers look for exactly this.

**Sharper than it first appears:** splitting at the *highlight* level is not
enough. Two highlights from the same book are near-duplicates in substance —
train on one, test on the other, and you have leakage that a highlight-level
split will not catch. **Split at the source level.**

**Unrecoverable after the fact.** Retrofitting a split post-training means
re-running everything. This is why it is Phase 1, not Phase 4.

## R3 — The eval set will be too small to show anything

With ~50 cases, a 5-point difference is noise. Small models are also
high-variance across prompt phrasings.

**Consequence:** you report "improvement" that a rerun would erase — the worst
possible outcome in a funding conversation.

**Mitigation:** enough cases to separate signal from noise; run each case
multiple times and report variance, not a single number; decide the threshold
that counts as improvement *before* seeing results.

## R4 — Judge variance masquerades as model change

Claude is not deterministic, and `temperature` is unavailable on Opus 5. Two
runs of the same answers can score differently.

**Mitigation:** structured output with an explicit rubric; score the *baseline
and candidate in the same run* so judge drift affects both equally; spot-check a
sample by hand. If judge disagreement approaches your effect size, the result is
not reportable.

## R5 — Retrieval will be the binding constraint, and hide your training gains

If retrieval hands the model the wrong passages, a better model cannot recover.
You will have fine-tuned successfully and measured nothing.

The current embeddings are **384-dim MiniLM-class** — tolerable at k=50, weak at
the k=6–10 a 4B context forces.

**Mitigation:** Phase 3 measures retrieval *in isolation* (recall@k) before the
model is involved. If recall@k is poor, fix retrieval first — training is wasted
effort until then.

## R6 — Attribution collapse

Change retrieval and weights together and no one can say which helped. This is
the most common way POCs become unpersuasive.

**Mitigation:** one variable per comparison; report the ablation grid, not a
single before/after.

## R7 — You will evaluate an artifact you do not serve

Train fp16 → merge → quantize to GGUF → serve via Ollama. **Quantization changes
behavior.** Evaluating the pre-quantization model reports a number the deployed
system does not produce.

**Mitigation:** invariant 5 — always evaluate through Ollama, on the exported
artifact.

## R8 — The PDF corpus does not exist yet

MinIO has no buckets. Any plan step depending on PDFs is currently unschedulable,
and PDF text extraction is its own project (layout, tables, OCR for scans) with
its own dependency decisions for the Airflow image.

**Mitigation:** treat PDFs as a second, later corpus. Nothing in Phases 0–5
requires them.

## R9 — Cross-context coupling

thesis-app belongs to the school context and the brain records that it shares
nothing with the data platform. This plan changes that.

**Mitigation:** one-way snapshot into `raw`, never live queries — business
infrastructure must not depend on a school app's uptime or schema. Record the
boundary change as a decision.

## R10 — `sources.extracted_text` is empty

Full source text is not in thesis-app; only highlighted excerpts and summaries.

**Consequence:** the corpus is pre-filtered to passages already chosen as
interesting. Good for signal-to-noise, but it means the model can only ever
reason over what was highlighted — and eval cases must not assume access to
un-highlighted content.

## R11 — Unsloth constrains base-model choice

Unsloth supports specific architectures. "A 4B model" is not a free parameter —
the base must be one Unsloth supports *and* that has a Blackwell-viable path
*and* that quantizes cleanly to GGUF for Ollama.

**Mitigation:** pick the base in Phase 0, against those three constraints
together, not later on capability grounds alone.

---

# 6. What would make me abandon this plan

Stated up front so the failure is recognisable rather than rationalised:

- Phase 0 cannot produce a working kernel on sm_120 after reasonable effort →
  the whole local-training premise needs revisiting.
- Retrieval recall@k stays poor after Phase 3 → the corpus or the chunking is
  wrong, and no amount of fine-tuning will show through.
- Judge variance is the same order as the effect size → the measurement cannot
  support the claim, regardless of how good the model is.
