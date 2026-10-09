"""Discover Wellcome's current workbook; run with python -m wellcome_awards."""
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

from lxml import html
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import download, http_session, run
from award_schemas import WELLCOME_SCHEMA

WELLCOME_PAGE = "https://wellcome.org/research-funding/funding-portfolio"


def discover_xlsx(page, base_url):
    candidates = set()
    for href in html.fromstring(page).xpath("//a/@href"):
        url = urljoin(base_url, href)
        parsed = urlparse(url)
        name = unquote(parsed.path.rsplit("/", 1)[-1]).lower()
        if name.startswith("wellcome-grants-awarded-") and name.endswith(".xlsx"):
            if parsed.scheme != "https" or parsed.hostname not in {"wellcome.org", "cms.wellcome.org"}:
                raise ValueError("Wellcome workbook points outside the expected HTTPS hosts")
            candidates.add(url)
    if len(candidates) != 1:
        raise ValueError(f"Expected one current Wellcome grants workbook, found {len(candidates)}; inspect {base_url}")
    return candidates.pop()


def process_xlsx(xlsx_path: Path, output_dir: Path) -> Path:
    """
    Process the Wellcome Trust XLSX into a parquet file.

    The XLSX has two sheets:
    - Main Grant List (we use this)
    - Additional Location Data (for multi-site grants)

    Args:
        xlsx_path: Path to the downloaded XLSX
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing XLSX")
    print(f"{'='*60}")

    print(f"  [READ] {xlsx_path.name} - Main Grant List")
    df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str, engine='openpyxl')

    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)[:10]}...")

    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace(':', '')
                  .str.replace('(', '')
                  .str.replace(')', '')
                  .str.replace('/', '_')
                  .str.replace('-', '_'))

    print(f"  Cleaned columns: {list(df.columns)}")


    column_mapping = {
        'identifier': 'identifier_360g',
        'title': 'title',
        'description': 'description',
        'currency': 'currency',
        'amount_awarded': 'amount',
        'award_date': 'award_date',
        'planned_datesstart_date': 'start_date',
        'planned_datesend_date': 'end_date',
        'recipient_orgidentifier': 'recipient_org_id',
        'recipient_orgname': 'recipient_org_name',
        'recipient_orgcountry_code': 'recipient_country',
        'recipient_orgpostal_code': 'recipient_postal_code',
        'funding_orgidentifier': 'funding_org_id',
        'funding_orgname': 'funding_org_name',
        'grant_programmetitle': 'grant_programme',
        'beneficiary_locationname': 'beneficiary_location',
        'beneficiary_locationcountry_code': 'beneficiary_country',
        'internal_id': 'internal_id',
        'lead_applicant': 'lead_applicant_name',
        'lead_applicantname': 'lead_applicant_name',
        'lead_applicantorcid': 'lead_applicant_orcid',
        'department': 'department',
        'school': 'school',
        'multi_location': 'multi_location',
    }

    rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=rename_dict)

    print(f"  Final columns: {list(df.columns)}")

    print("  [INFO] Building grant_ref (citable award id)...")
    if 'internal_id' not in df.columns:
        raise SystemExit(
            "[FATAL] 'Internal ID' column missing from Wellcome XLSX - "
            "cannot build citable grant_ref. Inspect the source file."
        )
    df['grant_ref'] = df['internal_id'].str.strip()
    fallback_mask = df['grant_ref'].isna() | (df['grant_ref'] == '')
    if fallback_mask.any() and 'identifier_360g' in df.columns:
        derived = (df.loc[fallback_mask, 'identifier_360g']
                   .str.replace('360G-Wellcome-', '', regex=False)
                   .str.replace('_', '/', regex=False))
        df.loc[fallback_mask, 'grant_ref'] = derived
        print(f"  [WARN] {fallback_mask.sum():,} rows had no Internal ID; derived from 360G identifier")

    shape_ok = df['grant_ref'].str.match(r'^\d{5,6}/[A-Z]/\d{2}/[A-Z]$', na=False)
    pct_ok = shape_ok.mean() * 100
    print(f"  grant_ref matching NNNNNN/L/NN/L shape: {shape_ok.sum():,}/{len(df):,} ({pct_ok:.1f}%)")
    if pct_ok < 95:
        print(df.loc[~shape_ok, ['grant_ref', 'identifier_360g']].head(10).to_string())
        raise SystemExit("[FATAL] <95% of grant_ref values match the Wellcome reference shape - inspect source")

    assert not df['grant_ref'].str.startswith('360G-', na=False).any(), \
        "360G-prefixed values leaked into grant_ref"

    print("  [INFO] Parsing dates...")

    if 'award_date' in df.columns:
        df['award_date'] = pd.to_datetime(df['award_date'], errors='coerce')
        df['award_date'] = df['award_date'].dt.strftime('%Y-%m-%d')
        df['award_date'] = df['award_date'].replace('NaT', None)

    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
        df['start_date'] = df['start_date'].replace('NaT', None)

    if 'end_date' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')
        df['end_date'] = df['end_date'].replace('NaT', None)

    print("  [INFO] Parsing amounts...")
    if 'amount' in df.columns:
        df['amount'] = df['amount'].str.replace(',', '', regex=False).str.strip()
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    print("  [INFO] Deduplicating by grant_ref...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['grant_ref'], keep='first')
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    if set(df.columns) != set(WELLCOME_SCHEMA.names):
        raise ValueError(f"Wellcome columns changed: {list(df.columns)}")
    df = df[WELLCOME_SCHEMA.names]
    output_path = output_dir / "wellcome.parquet"
    table = pa.Table.from_pandas(df, schema=WELLCOME_SCHEMA, preserve_index=False)
    pq.write_table(table, output_path)
    return output_path


def build(directory):
    with http_session() as session:
        response = session.get(WELLCOME_PAGE, timeout=(15, 120))
        response.raise_for_status()
        url = discover_xlsx(response.content, response.url)
        print(f"Discovered Wellcome workbook: {url}", flush=True)
        raw = directory / "wellcome.xlsx"
        download(session, url, raw)
    output = process_xlsx(raw, directory)
    raw.unlink()
    return output


if __name__ == "__main__":
    run("wellcome", build, WELLCOME_SCHEMA)
