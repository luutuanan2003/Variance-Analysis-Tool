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


EDIT BY AN from BWIDJSC