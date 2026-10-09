"""Gates committed grants CSV, monthly; origin conversion preserved."""
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from award_fetch import download, http_session, run, check_disk
from award_schemas import GATES_SCHEMA

GATES_CSV_URL = "https://www.gatesfoundation.org/-/media/files/bmgf-grants.csv"

def process_csv(csv_path: Path, output_dir: Path) -> Path:
    """
    Process the Gates Foundation CSV into a parquet file.

    The CSV has a metadata row first ("Updated X, YYYY"), then headers.

    Args:
        csv_path: Path to the downloaded CSV
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing CSV")
    print(f"{'='*60}")

    # Read CSV, skipping the first metadata row
    # The actual headers are on row 2 (index 1 after skip)
    print(f"  [READ] {csv_path.name}")
    df = pd.read_csv(csv_path, skiprows=1, dtype=str, encoding='utf-8-sig')

    print(f"  Total rows: {len(df):,}")
    # The vendor CSV includes one all-empty footer record; it is not an award.
    empty = df.isna().all(axis=1)
    print(f"  Empty CSV records omitted: {int(empty.sum())}")
    df = df.loc[~empty].copy()

    # Clean column names (lowercase, replace spaces)
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace('(', '')
                  .str.replace(')', ''))

    # Rename columns to match our schema
    column_mapping = {
        'grant_id': 'grant_id',
        'grantee': 'grantee_name',
        'purpose': 'purpose',
        'division': 'division',
        'date_committed': 'date_committed',
        'duration_months': 'duration_months',
        'amount_committed': 'amount',
        'grantee_website': 'grantee_website',
        'grantee_city': 'grantee_city',
        'grantee_state': 'grantee_state',
        'grantee_country': 'grantee_country',
        'region_served': 'region_served',
        'topic': 'topic'
    }
    df = df.rename(columns=column_mapping)

    # Parse and format date_committed (YYYY-MM format to YYYY-MM-DD)
    print("  [INFO] Parsing dates...")
    df['start_date'] = pd.to_datetime(df['date_committed'] + '-01', format='%Y-%m-%d', errors='coerce')
    df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
    df['start_date'] = df['start_date'].replace('NaT', None)

    # Calculate end_date from duration_months
    df['duration_months'] = pd.to_numeric(df['duration_months'], errors='coerce')

    # Create end_date by adding months to start_date
    start_dates = pd.to_datetime(df['date_committed'] + '-01', format='%Y-%m-%d', errors='coerce')
    df['end_date'] = start_dates + pd.to_timedelta(df['duration_months'] * 30.44, unit='D')
    df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d')
    df['end_date'] = df['end_date'].replace('NaT', None)

    # Clean amount (remove commas, convert to numeric)
    print("  [INFO] Parsing amounts...")
    df['amount'] = df['amount'].str.replace(',', '').str.strip()
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Remove duplicates by grant_id (keep first)
    print("  [INFO] Deduplicating by grant_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['grant_id'], keep='first')
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique grants: {len(df):,}")

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Define explicit schema for Spark compatibility
    # All string columns except amount (double) and duration_months (int)
    schema = GATES_SCHEMA
    if set(df.columns) != set(schema.names):
        raise ValueError(f"Gates columns changed: {list(df.columns)}")
    if df.grant_id.isna().any() or df.grant_id.str.strip().eq("").any():
        raise ValueError("Invalid Gates grant key")

    # Save to parquet with explicit schema
    output_path = output_dir / "gates_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Convert to pyarrow table with schema
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With purpose: {df['purpose'].notna().sum():,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With amount: {df['amount'].notna().sum():,}")
    print(f"    - Total amount: ${df['amount'].sum():,.0f}")

    if 'division' in df.columns:
        print(f"\n  Divisions:")
        print(df['division'].value_counts().head(10).to_string())

    if 'topic' in df.columns:
        print(f"\n  Topics (top 10):")
        print(df['topic'].value_counts().head(10).to_string())

    if 'grantee_country' in df.columns:
        print(f"\n  Countries (top 10):")
        print(df['grantee_country'].value_counts().head(10).to_string())

    return output_path

def build(directory):
    raw = directory / "gates.csv"
    with http_session() as session:
        download(session, GATES_CSV_URL, raw)
    output = process_csv(raw, directory)
    check_disk(directory)
    raw.unlink()
    return output


def main():
    run("gates", build, GATES_SCHEMA)


if __name__ == "__main__":
    main()
