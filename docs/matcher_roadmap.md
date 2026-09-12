# Matcher Roadmap — current design, 10 improvements, and the structural set-matching spec

*Scope: `crates/matcher`. The cross-venue matching engine is the strongest
"hard software" moat in the product — this doc records how it works today, the
ranked improvement backlog, and a detailed build spec for the highest-impact
item (#1, structural set-matching).*

---

## 1. How the matcher works today

A multi-pass, human-in-the-loop pipeline that runs **off the hot path** (startup
+ on a timer) and persists to a human-editable YAML **registry**
(`Active` / `Review` / `Rejected`). Human edits win and are reloaded each pass;
instruments already in an `Active` match are "pinned" and skipped.

**Pass 1 — Rule pass → `Exact` (deliberately conservative).**
- **Normalization** (`rules.rs`): lowercase, strip stopwords — but *keep*
  direction words (`above/below/over/under`), which distinguish opposite markets.
- **Anchor extraction** (regex): canonicalizes dates ("28 April 2027", Kalshi
  `26JUL` → `2027-04`), percentages (`4.25%`), USD (`$150k`), magnitudes (`1.5m`),
  bare numbers.
- **Exact** only on a normalized-key collision **and** a numeric/date anchor
  present **and** exactly one instrument per venue **and** close times within a
  tight guard (default 72h). A false `Exact` poisons divergence, so the bar is high.

**Pass 2 — Similarity pass → `High` / `Review`.**
- Cross-venue pairs only; close times within a window (default 48h); missing
  close time → kept with a 0.85× penalty.
- **Conflicting-anchor suppression**: drop "above 4.25%" vs "above 4.50%" outright
  (same event / different strike must never auto-match). *The horizon/strike-trap
  guard.*
- **Lexical scoring**: token-set **Jaccard** over normalized title (+ description).
- **Greedy 1:1**: best score first, one match per instrument, shortlist ≥ 0.35.
- **Optional LLM adjudication**: shortlisted pairs batched (16) to Claude Haiku →
  `same?` + confidence; ≥0.9 → `High`, ≥0.6 → `Review`. Without the key:
  lexical-only, capped at `Review`.

**Known structural limitation (the reason for #1):** the engine matches
instruments **1:1**. Real cross-venue equivalence is frequently **set↔set** — one
Kalshi multi-outcome event ↔ many Polymarket binaries; one Kalshi scalar/range
market ↔ a ladder of Polymarket threshold binaries. Those are invisible or
mis-scored today.

---

## 2. Improvement backlog (ranked by impact)

1. **Structural set-matching** (multi-outcome↔binary, scalar↔threshold). The #1
   gap and the core of the moat. **Full spec in §3.**
2. **Semantic-embedding shortlist** — replace/augment lexical Jaccard with vector
   embeddings + ANN so paraphrases ("Fed cuts rates" ↔ "FOMC lowers rate") reach
   adjudication. Biggest recall win; keep anchor/close-time guards for precision.
3. **Polarity / YES–NO inversion** — a Kalshi YES often equals a Polymarket NO;
   "Trump wins" ↔ "Trump does not win". Detect and **invert**, don't drop.
4. **Entity + alias normalization** — canonical dictionary (Trump/Donald Trump/DJT,
   BTC/Bitcoin, Fed/FOMC, tickers, countries, candidates). Lifts both passes.
5. **Resolution-criteria matching** — compare settlement *source and rules*, not
   just dates/anchors; downgrade rule-mismatched pairs to `Review`.
6. **Active-learning loop from the registry** — human Active/Rejected are labeled
   data; use them to tune thresholds, train a reranker, auto-suppress rejected
   patterns. Turns curation into a **compounding proprietary asset** (the durable
   moat, not just hard software).
7. **Learned reranker** — replace static thresholds (0.35/0.55/0.9) with a
   calibrated model over features (lexical, embedding, anchor agreement, close-time
   delta, entity overlap, structure).
8. **Smarter/cheaper LLM adjudication** — cache stable verdicts; escalate only
   *uncertain* pairs (lexical+embedding disagree); structured JSON/tool-use. Lets
   you widen the shortlist (recall) without cost blowup.
9. **Temporal / lifecycle tracking** — markets relist, rename, drift, resolve.
   Track match stability across passes, re-review on term drift, template recurring
   series (monthly CPI, weekly jobless claims).
10. **Confidence-aware handoff + review-queue UI** — never fire a hard cross-venue
    alert on a `Review`-grade match; weight by confidence; a fast human review queue
    that feeds #6.

**If you do three:** #1, #2, #6 — #1/#2 make it hard-to-replicate infra; #6 makes
it *compound*.

---

## 3. Detailed spec — #1 Structural set-matching

### 3.1 Motivation

Cross-venue equivalence is often **set↔set**, not 1:1:

- **Categorical (multi-outcome ↔ binaries).** Kalshi "Who wins the 2028 election?"
  is one mutually-exclusive event with a market per candidate; Polymarket has N
  separate binaries "Will {candidate} win 2028?". → 1 Kalshi event ↔ N Poly binaries.
- **Scalar/threshold (range ↔ ladder).** Kalshi "What will CPI be?" is a bucketed
  range (2.0–2.5%, 2.5–3.0%, …); Polymarket has threshold binaries ("CPI above 3%?").
  → 1 Kalshi range family ↔ a ladder of Poly thresholds.

Matching these unlocks **like-for-like comparables** for divergence/surveillance
(and new detection — §3.7). It is also the piece a generic team/AI cannot quickly
replicate, so it is the moat-defining work.

### 3.2 Goals / non-goals

**Goals:** detect market *families* per venue; type their outcome spaces; align
families across venues; emit a structural mapping with a **comparator** that yields
like-for-like probabilities for downstream detectors; stay human-in-the-loop and
confidence-graded like the existing registry.

**Non-goals (this phase):** trading/routing; resolving *legal* fungibility;
cross-lingual; anything on the hot path (this runs in the same off-path pass).

### 3.3 Data model (additions to `vw_core` + registry)

```
MarketFamily {
    family_id: FamilyId,            // stable id (venue + event/series key)
    venue: Venue,
    kind: FamilyKind,               // Binary | Categorical | ScalarLadder
    subject: NormalizedTitle,       // shared, strike/candidate-stripped title
    resolution: ResolutionMeta,     // date, source, rules (for §3.6 gating)
    members: Vec<FamilyMember>,
}

FamilyMember {
    instrument: InstrumentId,
    label: MemberLabel,             // Candidate(entity) | Threshold{op, strike} | Bucket{lo,hi} | YesNo
}

FamilyKind = Binary | Categorical | ScalarLadder

StructuralMatch {                   // new registry entry type (alongside pair matches)
    a_family: FamilyId,
    b_family: FamilyId,
    alignment: Vec<MemberLink>,     // how members correspond
    comparator: Comparator,         // how to derive comparable quantities (§3.5)
    confidence: MatchConfidence,
    status: MatchStatus,            // Active | Review | Rejected (human-editable)
    coverage: f64,                  // fraction of members successfully aligned
}

MemberLink {
    a: Option<InstrumentId>,        // None = unmatched member (partial ladder)
    b: Option<InstrumentId>,
    transform: LinkTransform,       // Identity | Invert(polarity) | BucketToThreshold{...}
}
```

### 3.4 Pipeline (extends the existing two-pass flow)

```
 instruments ──► [A. Family detection] ──► families per venue
                       │
                       ▼
                 [B. Family typing] ──► kind + member labels
                       │
                       ▼
   [C. Cross-venue family alignment] ──► StructuralMatch candidates
        (reuse title/anchor/entity/close-time + LLM machinery at FAMILY level)
                       │
                       ▼
        [D. Member alignment per kind] ──► MemberLinks + comparator
                       │
                       ▼
             [E. Merge into registry] ──► human review, confidence, coverage
```

**A. Family detection.**
- *Kalshi:* group by `series_ticker` / `event_ticker` (markets already nest under
  events); mutually-exclusive event groups are categorical, strike ladders are
  scalar.
- *Polymarket:* cluster binaries that share a normalized subject + resolution date
  but differ only by candidate entity (categorical) or by threshold strike
  (scalar). Use existing normalization + the new entity dictionary (#4) to strip
  the differing token.
- A singleton binary with no siblings stays `Binary` (existing 1:1 path).

**B. Family typing.**
- `Categorical` if members differ by a **mutually-exclusive entity** (candidate,
  outcome) and are (near-)exhaustive.
- `ScalarLadder` if members are ordered **thresholds/buckets** on one numeric
  underlying (reuse anchor extraction to read strikes).
- else `Binary`.

**C. Cross-venue family alignment.** Reuse the current machinery *lifted to the
family level*: normalized subject Jaccard + embedding sim (#2) + anchor/date
agreement + close-time guard + optional LLM adjudication ("are these two families
about the same underlying question?"). Emit `StructuralMatch` candidates with a
family-level confidence.

**D. Member alignment (per aligned family pair):**
- **Categorical ↔ Binaries:** map candidate↔binary by entity match (needs #4
  entity aliasing). Each Kalshi candidate market ↔ its Polymarket binary.
  `transform = Identity` (or `Invert` if one side is phrased negatively).
- **ScalarLadder ↔ Thresholds:** build a common strike grid. Map each Polymarket
  threshold `P(X > k)` to the Kalshi buckets: `P(X > k) = Σ P(bucket_i)` for
  buckets above `k`. `transform = BucketToThreshold{k, buckets_above}`.
  Handle mismatched/partial grids by interpolation and mark unmatched members
  (`coverage < 1`).
- **Binary ↔ Binary:** existing path, now also polarity-aware (#3).

**E. Merge.** Same registry semantics: stronger findings promote `Review`→`Active`;
rejected family pairs suppressed; `coverage` recorded so downstream can weight.

### 3.5 The comparator (why this matters)

The output isn't just "these sets correspond" — it's a **`Comparator`** that
produces **like-for-like probabilities** for the divergence/surveillance layer:

- **Categorical:** for each aligned candidate, compare Kalshi market price vs
  Polymarket binary price directly. Also compute the **set coherence**: does each
  venue's candidate set sum to ~1? A venue whose set sums to 1.15 has an internal
  arbitrage — itself a signal (§3.7).
- **ScalarLadder:** reconstruct the implied threshold probability from Kalshi
  buckets (`Σ` of buckets above `k`) and compare to Polymarket's `P(X>k)` binary.
  *(This is the digital-from-buckets identity — the same Breeden–Litzenberger idea
  used in the sibling event-hedge-lab: a threshold is a sum/limit of buckets.)*

The comparator is what turns structural matches into a stream of comparable
`(venue_a_prob, venue_b_prob)` observations the existing divergence detector can
consume — with a `coverage`/confidence weight.

### 3.6 Edge cases (must handle, or explicitly `Review`)

- **Non-exhaustive sets** — Polymarket binaries that don't cover every candidate,
  or a "field/other" bucket. Track `coverage`; don't force a full mapping.
- **Different strike grids** — Kalshi buckets vs Polymarket thresholds that don't
  line up; interpolate and flag reduced confidence.
- **Bundled vs split outcomes** — "any Democrat" (one side) vs per-candidate (other).
- **Per-member resolution mismatch** — one candidate/threshold resolves on a
  different source; gate with #5, downgrade to `Review`.
- **Overlapping/mutually-non-exclusive** Polymarket binaries that look like a set
  but aren't (two thresholds that both can be YES). Detect via label logic.

### 3.7 Surveillance payoff (why this is worth building first)

Structural matching **unlocks new detection**, not just better coverage:

- **Cross-venue set divergence** — the whole distribution (all candidates/thresholds)
  diverging, not just one binary — a stronger, harder-to-noise signal.
- **Coherence / internal-arbitrage checks** — a candidate set that doesn't sum to 1,
  or a threshold ladder that's non-monotonic, is a manipulation/mispricing tell
  *within* a venue — detectable only once you understand the family structure.
- **Bigger, cleaner cross-venue surface** — categorical and scalar families are
  exactly the marquee, liquid markets (elections, macro, prices) where manipulation
  matters most.

### 3.8 Phasing & milestones

- **Phase A — families:** detection + typing per venue; unit tests on real Kalshi
  events + Polymarket binary clusters. *(No matching yet — just structure.)*
- **Phase B — categorical alignment:** elections first (cleanest, highest value);
  entity-aliased candidate↔binary mapping + coherence comparator.
- **Phase C — scalar alignment:** CPI/prices; bucket→threshold reconstruction +
  interpolation for mismatched grids.
- **Phase D — wire to detection:** feed comparator observations into the divergence
  detector with coverage/confidence weighting.
- **Phase E — set-level + coherence signals** as new detectors.

### 3.9 Testing & acceptance

- **Fixtures:** committed real Kalshi multi-outcome events (an election, a Fed-rate
  ladder) + the corresponding Polymarket binaries. Deterministic, offline.
- **Acceptance:**
  - Phase A: correctly types ≥90% of a hand-labeled fixture set of families.
  - Phase B: on a real election, aligns candidate↔binary at ≥95% precision, coverage
    reported; coherence check flags an intentionally-broken (sum≠1) fixture.
  - Phase C: reconstructed `P(X>k)` from Kalshi buckets matches a hand-computed
    value within tolerance; partial grids reported, not silently wrong.
  - No `Active` structural match is emitted below the family-confidence bar; every
    borderline case lands in `Review`.
- **Guardrail (unchanged principle):** a false structural `Active` poisons
  divergence worse than a missing one — bias to `Review`, keep the human in the loop.

### 3.10 Effort (rough)

Phase A–B (categorical, the high-value 80%): **~2–4 focused weeks**. Phase C
(scalar reconstruction) adds ~1–2 weeks. Phases D–E are smaller once the mapping
+ comparator exist. This is genuine, non-trivial infra — which is exactly why it's
the moat.

---

*Cross-reference: the digital-from-buckets reconstruction in §3.5 mirrors the
Breeden–Litzenberger / Carr–Madan machinery in the sibling `event-hedge-lab` — the
same math, reused to make cross-venue scalar markets comparable.*
