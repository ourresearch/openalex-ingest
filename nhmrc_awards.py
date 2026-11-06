"""NHMRC (Australia) grant outcomes as one object. A manual source: no key, and no download from NHMRC.

NHMRC publishes its outcomes only as per-round "Summary of results" XLSX files on www.nhmrc.gov.au, whose Akamai front
refuses non-browser clients (2026-11-05: HTTP/2 INTERNAL_ERROR, HTTP/1.1 read timeouts, even for robots.txt). So for each
release a person downloads the round's file in a browser and copies it, under NHMRC's own file name, to
s3://openalex-ingest/awards/manual/nhmrc/; then `python -m nhmrc_awards` writes one object from:
  - the two objects walden served NHMRC from until now (read-only, fixed keys, never rewritten):
      awards/nhmrc/nhmrc_projects.parquet              "primary": 2013-2025 rounds (walden's laptop converter, 2026-01-08)
      awards/nhmrc/nhmrc_historical_1993_2016.parquet  "historical": the 1993-2016 all-grants file (2026-08-06)
  - every *.xlsx under awards/manual/nhmrc/, parsed as scripts/local/nhmrc_to_s3.py did (header-row search, column
    standardisation, the round year from the file name). Their rows replace primary rows with the same grant id; between
    files, the later round wins, then the later upload. "The same grant id" is one canonical form (trimmed, lower case) for
    both the dedupe between drops and the replacement of primary rows, so a re-issued " 2043672 " is the grant 2043672.
The outcome lists' own row counts (rows with a grant id, per file) go in the parquet footer (key openalex.funder_total)
for the gate.
Output: `part` ('primary' | 'historical'), the primary file's 64 columns in its order, then the 6 historical-only columns;
every value a string. walden's sources/nhmrc.sql maps both parts (the old CreateNHMRCAwards notebook, cells 9 and 11).
"""
from datetime import datetime, timezone
import json
import re

import boto3
from botocore.config import Config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import check_disk, run
from award_schemas import NHMRC_SCHEMA

BUCKET = "openalex-ingest"
PRIMARY_KEY = "awards/nhmrc/nhmrc_projects.parquet"
HISTORICAL_KEY = "awards/nhmrc/nhmrc_historical_1993_2016.parquet"
DROP_PREFIX = "awards/manual/nhmrc/"
PRIMARY_MIN_ROWS = 11902      # the frozen objects' row counts: a short read is refused
HISTORICAL_MIN_ROWS = 21000   # the old notebook's own shrink guard (cell 4)
HISTORICAL_ONLY = ["award_id", "application_year", "title", "pi_name", "institution", "budget_total"]
PRIMARY_COLUMNS = [n for n in NHMRC_SCHEMA.names if n != "part" and n not in HISTORICAL_ONLY]
HISTORICAL_COLUMNS = ["award_id", "application_year", "title", "pi_name", "grant_type", "institution",
                      "start_year", "end_year", "budget_total"]
REQUIRED_XLSX = ("grant_id", "grant_title", "cia_name")   # after standardisation (grant_id falls back to app_id)

# scripts/local/nhmrc_to_s3.py standardize_columns, unchanged.
COLUMN_MAPPING = {
    'grant_id': 'grant_id', 'grantid': 'grant_id',
    'app_id': 'app_id', 'appid': 'app_id', 'application_id': 'app_id',
    'grant_title': 'grant_title', 'title': 'grant_title', 'application_title': 'grant_title',
    'project_title': 'grant_title', 'scientific_title': 'grant_title', 'simplified_title': 'simplified_title',
    'cia': 'cia_name', 'cia_name': 'cia_name', 'chief_investigator_a': 'cia_name', 'chief_investigator': 'cia_name',
    'ci_a': 'cia_name', 'chief_investigator_a_project_lead': 'cia_name',
    'cia_orcid_id': 'cia_orcid',
    'administering_institution': 'administering_institution', 'admin_institution': 'administering_institution',
    'institution': 'administering_institution',
    'grant_value': 'total', 'total_budget': 'total', 'amount': 'total', 'funded_amount': 'total', 'total': 'total',
    'total_amount_awarded': 'total',
    'grant_type': 'grant_type', 'scheme': 'grant_type', 'funding_scheme': 'grant_type', 'funding_type': 'grant_type',
    'grant_sub_type': 'grant_sub_type', 'sub_type': 'grant_sub_type', 'category': 'grant_sub_type',
    'level_stream_or_sub_type': 'grant_sub_type', 'funding_sub_type': 'grant_sub_type',
    'start_date': 'start_date', 'grant_start_date': 'start_date', 'start_year': 'start_year', 'start_yr': 'start_year',
    'end_date': 'end_date', 'grant_end_date': 'end_date', 'end_year': 'end_year', 'end_yr': 'end_year',
    'date_announced': 'date_announced', 'app_year': 'app_year', 'application_year': 'app_year',
    'state_territory': 'state_territory', 'state': 'state_territory', 'state_or_territory': 'state_territory',
    'broad_research_area': 'broad_research_area', 'bra': 'broad_research_area',
    'for': 'fields_of_research', 'fields_of_research': 'fields_of_research', 'field_of_research': 'fields_of_research',
    'field_s_of_research': 'fields_of_research',
    'plain_description': 'plain_description',
    'sector': 'sector', 'organisation_type': 'sector',
}


class S3Inputs:
    """The production inputs: two frozen objects and the drop folder, read-only."""

    def __init__(self, directory):
        self.directory = directory
        self.s3 = boto3.client("s3", config=Config(retries={"mode": "standard", "max_attempts": 5}))

    def _table(self, key):
        path = self.directory / key.rsplit("/", 1)[-1]
        self.s3.download_file(BUCKET, key, str(path))
        table = pq.read_table(path)
        path.unlink()
        return table

    def primary(self):
        return self._table(PRIMARY_KEY)

    def historical(self):
        return self._table(HISTORICAL_KEY)

    def drops(self):
        """[(file name, last modified ISO string, local path)] for every .xlsx in the drop folder."""
        found = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=DROP_PREFIX):
            for obj in page.get("Contents", []):
                name = obj["Key"][len(DROP_PREFIX):]
                if "/" in name or not name.lower().endswith(".xlsx"):
                    continue
                check_disk(self.directory)
                path = self.directory / f"drop-{len(found)}.xlsx"
                self.s3.download_file(BUCKET, obj["Key"], str(path))
                found.append((name, obj["LastModified"].isoformat(), path))
        return found


def round_year(name):
    """The round year from NHMRC's file name (nhmrc_to_s3.py find_existing_files, the same patterns in order)."""
    for pattern in (r'nhmrc_(\d{4})\.xlsx', r'^(\d{4})-application-round\.xlsx', r'[Ss]ummary[-_]of[-_]results?[-_](\d{4})[-_]',
                    r'(20[1-3]\d)'):
        match = re.search(pattern, name)
        if match:
            return int(match.group(1))
    raise ValueError(f"NHMRC drop {name!r}: no round year in the file name; keep NHMRC's own file name")


def _dedupe_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        for i, idx in enumerate(cols[cols == dup].index.tolist()[1:], 1):
            cols.iloc[idx] = f"{dup}_{i}"
    df.columns = cols
    return df


def _header_row(preview):
    known = ['grant id', 'app id', 'application id', 'grant title', 'cia', 'grant value']
    for idx in range(min(10, len(preview))):
        values = preview.iloc[idx].astype(str).str.lower().tolist()
        if sum(1 for h in known if any(h in str(v) for v in values)) >= 2:
            return idx
    return 0


def parse_xlsx(name, path):
    """One outcomes workbook -> standardised rows (strings), as nhmrc_to_s3.py parse_xlsx did; fails instead of skipping."""
    year = round_year(name)
    with pd.ExcelFile(path) as workbook:
        sheets = workbook.sheet_names
    sheet = 'GRANTS DATA' if 'GRANTS DATA' in sheets else ('2013 App Data' if '2013 App Data' in sheets else 0)
    header = _header_row(pd.read_excel(path, sheet_name=sheet, header=None, nrows=15, dtype=str))
    df = _dedupe_columns(pd.read_excel(path, sheet_name=sheet, header=header, dtype=str)).dropna(how='all')
    df.columns = (df.columns.astype(str).str.lower().str.strip().str.replace(r'\s+', '_', regex=True)
                  .str.replace(r'[^\w]', '_', regex=True).str.replace(r'_+', '_', regex=True).str.strip('_'))
    df = _dedupe_columns(df).rename(columns=COLUMN_MAPPING)
    df = df.loc[:, ~pd.Index(df.columns).duplicated()]   # two headers mapped to one name: the first wins
    if 'grant_title' not in df.columns and 'simplified_title' in df.columns:
        df['grant_title'] = df['simplified_title']
    if 'grant_id' not in df.columns and 'app_id' in df.columns:
        df['grant_id'] = df['app_id']
    missing = [c for c in REQUIRED_XLSX if c not in df.columns]
    if missing:
        raise ValueError(f"NHMRC drop {name!r}: no {missing} column after standardisation; check the workbook's headers")
    df['application_round_year'] = str(year)
    df = df[df['grant_id'].notna() & (df['grant_id'].astype(str).str.strip() != '')]
    if df.empty:
        raise ValueError(f"NHMRC drop {name!r}: no rows with a grant id")
    dropped = sorted(c for c in df.columns if c not in PRIMARY_COLUMNS)
    return year, df[[c for c in df.columns if c in PRIMARY_COLUMNS]], dropped


def canonical_ids(ids):
    """The one grant-id form used for both the drop dedupe and the primary replacement."""
    return ids.astype(str).str.strip().str.lower()


def _strings(df, columns):
    out = pd.DataFrame({c: df[c] if c in df.columns else None for c in columns}, index=df.index, dtype=object)
    for c in columns:
        out[c] = [None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v) for v in out[c]]
    return out


def build(directory, inputs=None):
    inputs = inputs or S3Inputs(directory)
    output = directory / "nhmrc_outcomes.parquet"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    primary = inputs.primary().to_pandas()
    historical = inputs.historical().to_pandas()
    if len(primary) < PRIMARY_MIN_ROWS or len(historical) < HISTORICAL_MIN_ROWS:
        raise ValueError(f"NHMRC frozen inputs short: {len(primary)} primary, {len(historical)} historical")
    if list(historical.columns) != HISTORICAL_COLUMNS or set(primary.columns) - set(PRIMARY_COLUMNS):
        raise ValueError("NHMRC frozen inputs' columns changed")
    files = []
    for name, modified, path in inputs.drops():
        year, rows, dropped = parse_xlsx(name, path)
        files.append((year, modified, name, rows))
        print(json.dumps({"nhmrc_drop": name, "round": year, "rows": len(rows), "unmapped_columns": dropped}), flush=True)
    # Later round first, then the later upload: the first row per grant id wins (nhmrc_to_s3.py keep="first").
    files.sort(key=lambda f: (f[0], f[1], f[2]), reverse=True)
    dropped_rows = pd.concat([f[3] for f in files], ignore_index=True) if files else pd.DataFrame(columns=PRIMARY_COLUMNS)
    if len(dropped_rows):
        dropped_rows = dropped_rows[~canonical_ids(dropped_rows['grant_id']).duplicated(keep='first')].copy()
        dropped_rows['ingested_at'] = stamp
    replaced = set(canonical_ids(dropped_rows['grant_id'])) if len(dropped_rows) else set()
    keep = ~canonical_ids(primary['grant_id']).isin(replaced)
    parts = [_strings(dropped_rows, PRIMARY_COLUMNS), _strings(primary[keep], PRIMARY_COLUMNS)]
    prim = pd.concat(parts, ignore_index=True)
    prim.insert(0, 'part', 'primary')
    hist = _strings(historical, HISTORICAL_COLUMNS)
    hist.insert(0, 'part', 'historical')
    listed = sum(len(f[3]) for f in files)
    funder_total = {"measure": "rows with a grant id in NHMRC's outcome lists (the dropped XLSX files)", "value": listed,
                    "excluded": {"grant id repeated within or across the lists (the later one kept)": listed - len(dropped_rows)},
                    "included": {"frozen primary rows kept (2013-2025 object)": int(keep.sum()),
                                 "frozen historical rows (1993-2016 object)": len(hist)},
                    "files": [{"file": f[2], "round": f[0], "list_rows": len(f[3])} for f in files]}
    schema = NHMRC_SCHEMA.with_metadata({"openalex.funder_total": json.dumps(funder_total)})   # the gate reads it
    table = pa.Table.from_pandas(pd.concat([prim, hist], ignore_index=True)[NHMRC_SCHEMA.names],
                                 schema=schema, preserve_index=False)
    check_disk(directory)
    pq.write_table(table, output)
    print(json.dumps({"nhmrc_primary_rows": len(prim), "replaced_or_added_by_drops": len(dropped_rows),
                      "baseline_rows_replaced": int((~keep).sum()), "historical_rows": len(hist),
                      "drops": [f[2] for f in files]}), flush=True)
    return output


def main():
    run("nhmrc_outcomes", build, NHMRC_SCHEMA)


if __name__ == "__main__":
    main()
