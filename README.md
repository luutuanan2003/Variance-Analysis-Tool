Final v72 with smart fallback: use Total rows if available; otherwise sum details.

Browser
  │  POST /process  (multipart/form-data)
  │   - excel_files[] : .xlsx bytes (1..n)
  │   - mapping_file? : .xlsx bytes (0..1)
  │   - thresholds    : numbers/strings
  ▼
FastAPI (app/main.py)
  - Read all uploads into memory → excel_blobs=[(filename, bytes), ...]
  - mapping_blob = (filename, bytes) or None
  - Call core.process_all(...)

core.process_all(...)  (app/core.py)
  1) Build CONFIG from defaults + overrides
  2) Read mapping (if any) → corr_rules / season_rules
  3) For each (fname, fbytes) in excel_blobs:
       a) sub = extract_subsidiary_name_from_bytes(fbytes, fname)
       b) (bs_df, bs_cols) = process_financial_tab_from_bytes(fbytes, "BS Breakdown", "BS", sub)  [best-effort]
       c) (pl_df, pl_cols) = process_financial_tab_from_bytes(fbytes, "PL Breakdown", "PL", sub)  [best-effort]
       d) anoms_sub = build_anoms(sub, bs_df, bs_cols, pl_df, pl_cols, corr_rules, season_rules, CONFIG)
       e) collect: anomalies + (sub, bs_df, pl_df)
  4) Build ONE Excel workbook in memory:
       - Sheet "Anomalies Summary" (all anomalies)
       - Sheets "<sub>_BS" / "<sub>_PL" (cleaned tables, if available)
  5) Return workbook bytes

FastAPI
  - Wrap bytes in StreamingResponse (Excel MIME)
  - Content-Disposition: attachment; filename="variance_output.xlsx"
  ▼
Browser
  - Receives a single .xlsx download (this run only)

| Rule                           | Applies To                                                                            | Trigger (simplified)                                                                | Threshold Key(s)                                                        | Implemented In                    | Notes                                                           |
| ------------------------------ | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------- | --------------------------------------------------------------- |
| **BS movement**                | Balance Sheet accounts                                                                | \|Δ\| ≥ `materiality_vnd` **AND** \|%Δ\| > `bs_pct_threshold`                       | `materiality_vnd`, `bs_pct_threshold`                                   | `build_anoms`                     | Generic BS spike; suggests reclass/missing offset.              |
| **PL – Depreciation (%-only)** | PL accounts with prefixes in `dep_pct_only_prefixes` (e.g., 217, 632)                 | \|%Δ\| > `recurring_pct_threshold`                                                  | `dep_pct_only_prefixes`, `recurring_pct_threshold`                      | `build_anoms`                     | No absolute gate; depreciation flagged by % only.               |
| **PL – Recurring**             | PL “Recurring” accounts (prefixes in `recurring_code_prefixes`, e.g., 6321, 635, 515) | \|Δ\| ≥ `materiality_vnd` **AND** \|%Δ\| > `recurring_pct_threshold`                | `recurring_code_prefixes`, `materiality_vnd`, `recurring_pct_threshold` | `build_anoms`                     | Accrual/timing sensitive items.                                 |
| **PL – Revenue/OPEX**          | Other PL accounts (e.g., 511, 641, 642 etc.)                                          | \|%Δ\| > `revenue_opex_pct_threshold` **OR** \|Δ\| ≥ `materiality_vnd`              | `revenue_opex_pct_threshold`, `materiality_vnd`                         | `build_anoms`                     | Catches % spikes even if small in VND, or large absolute moves. |
| **Gross Margin drop**          | GM% from 511\* vs 632\*                                                               | GM% drop ≥ `gm_drop_threshold_pct` (absolute pp)                                    | `gm_drop_threshold_pct`                                                 | `build_gross_margin_anoms`        | Notes include prev→curr GM%.                                    |
| **Revenue by customer**        | 511\* grouped by detected customer column                                             | \|%Δ\| > `revenue_opex_pct_threshold` **OR** \|Δ\| ≥ `materiality_vnd`              | `revenue_opex_pct_threshold`, `materiality_vnd`                         | `build_revenue_by_customer_anoms` | Requires a customer column (auto-detected via hints).           |
| **Correlation break**          | Left patterns ↔ Right patterns across BS+PL                                           | Expected directional/inverse relation broken **AND** \|Left Δ\| ≥ `materiality_vnd` | `materiality_vnd` (plus corr\_rules sheet)                              | `build_corr_anoms`                | Rules come from `corr_rules` DataFrame (patterns + relation).   |
| **GM Δ≥1pp**                   | 511\* & 632\* (PL pivot)                                                              | \|GM% m/m Δ\| ≥ `gross_margin_pct_delta`                                            | `ACCT_THRESH.gross_margin_pct_delta`                                    | `check_gross_margin`              | Similar to GM drop, via accounting wrapper path.                |
| **P\&L Depreciation Δ%**       | 632\* total (PL pivot)                                                                | \|%Δ\| ≥ `depr_pct_delta`                                                           | `ACCT_THRESH.depr_pct_delta`                                            | `check_depreciation_variance`     | PL-side depreciation volatility.                                |
| **BS 217* Δ%*\*                | 217\* (Acc. Depreciation)                                                             | \|%Δ\| ≥ `depr_pct_delta`                                                           | `ACCT_THRESH.depr_pct_delta`                                            | `check_depreciation_variance`     | BS-side acc. depreciation swings.                               |
| **BS 214* Δ%*\*                | 214\* (Acc. Depreciation SCC)                                                         | \|%Δ\| ≥ `depr_pct_delta`                                                           | `ACCT_THRESH.depr_pct_delta`                                            | `check_depreciation_variance`     | Optional if 214\* exists.                                       |
| **COGS/Revenue ratio drift**   | 632\* vs 511\* (PL pivot)                                                             | \| (COGS/Rev) − hist\_mean \| ≥ `cogs_ratio_delta`                                  | `ACCT_THRESH.cogs_ratio_delta`                                          | `check_cogs_vs_revenue_ratio`     | Uses historical mean as baseline.                               |
| **SG\&A % of Revenue spike**   | 641\*/642\* vs 511\* (PL pivot)                                                       | (SG\&A/Rev) > mean × (1 + `sga_pct_of_rev_delta`)                                   | `ACCT_THRESH.sga_pct_of_rev_delta`                                      | `check_sga_as_pct_of_revenue`     | Relative to historical mean % of revenue.                       |
| **Financial swings**           | 515\* (income), 635\* (expenses)                                                      | \|%Δ\| ≥ `fin_swing_pct`                                                            | `ACCT_THRESH.fin_swing_pct`                                             | `check_financial_items_swings`    | Captures interest/FX volatility.                                |
| **BS↔PL Dep mismatch**         | Δ(217\*+214\*) vs Δ(632\*)                                                            | mismatch% > `bs_pl_dep_diff_pct`                                                    | `ACCT_THRESH.bs_pl_dep_diff_pct`                                        | `check_bs_pl_dep_consistency`     | Reconciles BS accumulated vs PL expense.                        |


EDIT BY AN from BWIDJSC