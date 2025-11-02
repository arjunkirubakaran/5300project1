# Project 01 — Single-Block SQL Heuristic Optimizer

This program parses a **single-block SQL query** and applies **heuristic logical optimizations**,
printing the **canonical query tree**, **each optimization step**, and the **final optimized tree**.
(Extra credit) It can also **regenerate** an optimized SQL query from the final tree and supports **basic unnesting** rewrites.

---

## ✨ Features
- Canonical logical algebra tree from SQL (`σ`, `π`, `⨝`, `γ`, `τ` for select, project, join, group, order).
- Step-by-step transformations with diffs:
  - Break up conjunctive predicates
  - Selection pushdown (to base relations and through joins when safe)
  - Projection pushdown (derive minimal attribute set)
  - Convert cross-product + selection → equi-join
  - Cascade / combine selections
  - HAVING → WHERE when legal
  - Early aggregation (when functional dependencies allow; simplified check)
  - Join reordering (simple greedy heuristic: relations with selections and equi-keys first)
  - Redundant predicate elimination (basic)
- (Extra) **Unnesting**:
  - `IN` / `EXISTS` → **semijoin**
  - `NOT IN` / `NOT EXISTS` → **antijoin**
- (Extra) **SQL regeneration** from the optimized tree.

> Note: This is a **didactic** optimizer, not a full DBMS. It handles common patterns of single-block queries.
  Complex edge cases are intentionally out-of-scope. The code is well-commented to ease extension.

---

## 🧪 Example
```
python -m src.main --sql examples/query1.sql --emit-sql optimized.sql
```

**Output** (abridged):
- `Step 0 — Canonical Tree`
- `Step 1 — BreakUpConjuncts`
- `Step 2 — SelectionPushdown`
- `Step 3 — ProjectionPushdown`
- `Step 4 — JoinizeSelections`
- `Step 5 — ReorderJoins`
- `Step 6 — HavingToWhere`
- `Step 7 — Final Optimized Tree`
- (optional) `optimized.sql`

---

## 📦 Input Requirements
- **Single-block SQL** comprised of:
  - `SELECT` (attributes, aggregates `SUM/COUNT/AVG/MIN/MAX`)
  - `FROM` (1+ relations; table aliases supported: `FROM Employee E`)
  - `WHERE` (conjunctive/disjunctive predicates; CNF preferred but not required)
  - `GROUP BY`, `HAVING`, `ORDER BY`

(Extra) Basic unnesting for common `IN/EXISTS` forms.

Assumptions: Schema and attribute references are flat (`T.col` or `col`). No UDFs/window functions.

---

## 🖨️ Output
- Canonical query tree (pretty-printed, textual).
- A **trace** of each major transformation rule with the updated tree.
- Final optimized tree.
- (Extra) Regenerated SQL (if `--emit-sql` is provided).

---

## 🚀 Build & Run
### 1) Environment
```
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2) Run
```
python -m src.main --sql examples/query1.sql --emit-sql optimized.sql
```

Flags:
```
--sql <path>           Path to SQL file OR inline SQL string.
--inline               Treat `--sql` as the literal SQL (not a path).
--no-unnest            Disable extra-credit unnesting rewrites.
--no-sqlout            Skip SQL regeneration.
--trace                Show detailed rule-by-rule logs (default on).
```

---

## 🧱 Project Structure
```
Project01_SQL_Heuristic_Optimizer/
├── requirements.txt
├── README.md
├── examples/
│   └── query1.sql
└── src/
    ├── main.py
    ├── parser_front.py
    ├── logical_tree.py
    ├── rules.py
    ├── unnesting.py
    └── sqlify.py
```

---

## 🔬 Testing
```
python -m pytest -q
```
*(A tiny sanity test is included.)*

---

## 🧠 Notes on Heuristics
The order of heuristic application matters. A reasonable pipeline is:

1. **BreakUpConjuncts** in `WHERE` → list of `σ` nodes per predicate.
2. **Pushdown Selection** close to base relations; push through joins when predicate references only one side.
3. **Joinize**: convert `σ(condition on A.x = B.y)` above `×` into `A ⨝ B` with join predicate.
4. **Pushdown Projection** using a **needed attributes** walk.
5. **Reorder Joins** (greedy): relations with selective predicates and with equi-join keys earlier.
6. **HAVING → WHERE** when no aggregates used in the predicate.
7. **Aggregate / Group** early when safe (simplified: only if all non-aggregated attrs are in GROUP BY).
8. **Order** stays top-most (`τ`) as it affects only presentation.

---

## 📜 License
MIT (for classroom use).
