"""CORDIS bulk programmes, monthly; mapping ported from Walden origin.

The current JSON ZIP contains per-project documents, not the origin's flat
project/organization arrays. CSV ZIPs expose those original fields; decimal
commas are converted before applying the unchanged output mapping.
"""
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from award_fetch import download, http_session, run, check_disk
import csv
import io
import zipfile
from award_schemas import CORDIS_SCHEMA

CORDIS_DATASETS = {
    "horizon_europe": {
        "name": "Horizon Europe (2021-2027)",
        "projects_url": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
        "framework": "HORIZON",
    },
    "h2020": {
        "name": "Horizon 2020 (2014-2020)",
        "projects_url": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
        "framework": "H2020",
    },
    "fp7": {
        "name": "FP7 (2007-2013)",
        "projects_url": "https://cordis.europa.eu/data/cordis-fp7projects-csv.zip",
        "framework": "FP7",
    },
}

def get_coordinator_map(organizations: list) -> dict:
    """
    Create a mapping of project_id -> coordinator organization.

    Args:
        organizations: List of organization dictionaries

    Returns:
        Dict mapping project_id to coordinator organization dict
    """
    coord_map = {}
    for org in organizations:
        if org.get("role") == "coordinator":
            project_id = org.get("projectID")
            if project_id:
                coord_map[project_id] = org
    return coord_map

def process_projects(
    projects: list,
    organizations: list,
    framework: str
) -> list[dict]:
    """
    Process raw CORDIS data into normalized records.

    Args:
        projects: List of project dictionaries from CORDIS
        organizations: List of organization dictionaries
        framework: Framework programme name (H2020, HORIZON, FP7)

    Returns:
        List of processed project dictionaries
    """
    # Build coordinator lookup
    coord_map = get_coordinator_map(organizations)

    processed = []
    for proj in projects:
        project_id = proj.get("id")

        # Get coordinator info
        coord = coord_map.get(project_id, {})

        record = {
            # Project identifiers
            "cordis_project_id": project_id,
            "acronym": proj.get("acronym"),
            "rcn": proj.get("rcn"),
            "grant_doi": proj.get("grantDoi"),

            # Project details
            "title": proj.get("title"),
            "objective": proj.get("objective"),
            "status": proj.get("status"),
            "framework_programme": framework,
            "funding_scheme": proj.get("fundingScheme"),
            "topics": proj.get("topics"),
            "legal_basis": proj.get("legalBasis"),
            "master_call": proj.get("masterCall"),
            "sub_call": proj.get("subCall"),
            "keywords": proj.get("keywords"),

            # Dates
            "start_date": proj.get("startDate"),
            "end_date": proj.get("endDate"),
            "ec_signature_date": proj.get("ecSignatureDate"),

            # Funding
            "total_cost": proj.get("totalCost"),
            "ec_max_contribution": proj.get("ecMaxContribution"),

            # Coordinator organization
            "coordinator_name": coord.get("name"),
            "coordinator_short_name": coord.get("shortName"),
            "coordinator_country": coord.get("country"),
            "coordinator_city": coord.get("city"),
            "coordinator_org_id": coord.get("organisationID"),
            "coordinator_activity_type": coord.get("activityType"),
            "coordinator_ec_contribution": coord.get("ecContribution"),

            # Metadata
            "content_update_date": proj.get("contentUpdateDate"),
        }
        processed.append(record)

    return processed

def process_dataframe(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Process DataFrame and save as parquet.

    Args:
        df: Raw DataFrame
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing data")
    print(f"{'='*60}")

    print(f"  Total rows: {len(df):,}")

    # Convert dates to string format (YYYY-MM-DD)
    print("  Converting dates to string format...")
    date_columns = ["start_date", "end_date", "ec_signature_date"]
    for col in date_columns:
        if col in df.columns:
            # Handle various date formats
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
            df[col] = df[col].replace("NaT", None)

    # Convert numeric fields
    print("  Converting numeric fields...")
    numeric_columns = ["total_cost", "ec_max_contribution", "coordinator_ec_contribution"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype("string").str.replace(",", ".", regex=False), errors="coerce")

    # Ensure string columns are properly typed (avoid mixed types)
    print("  Cleaning string columns...")
    string_columns = [
        "acronym", "title", "objective", "status", "framework_programme",
        "funding_scheme", "topics", "legal_basis", "master_call", "sub_call",
        "keywords", "coordinator_name", "coordinator_short_name",
        "coordinator_country", "coordinator_city", "coordinator_activity_type",
        "grant_doi", "content_update_date", "coordinator_org_id", "cordis_project_id", "rcn"
    ]
    for col in string_columns:
        if col in df.columns:
            # Convert all values to string, replacing None/NaN with None
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None else None)

    # Remove duplicates by cordis_project_id
    print("  Deduplicating by project ID...")
    original_count = len(df)
    df = df.drop_duplicates(subset=["cordis_project_id"], keep="first")
    if original_count > len(df):
        print(f"    Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Add metadata
    df["ingested_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Save to parquet
    output_path = output_dir / "european_commission_projects.parquet"
    print(f"\n  Saving to {output_path.name}...")
    if set(df.columns) != set(CORDIS_SCHEMA.names):
        raise ValueError("CORDIS output columns changed")
    for field in CORDIS_SCHEMA:
        if pa.types.is_null(field.type) and df[field.name].notna().any():
            raise ValueError(f"Populated formerly null-only CORDIS field: {field.name}")
    pq.write_table(pa.Table.from_pandas(df, schema=CORDIS_SCHEMA, preserve_index=False), output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df):,}")
    print(f"    - With title: {df['title'].notna().sum():,}")
    print(f"    - With objective: {df['objective'].notna().sum():,}")
    print(f"    - With dates: {df['start_date'].notna().sum():,}")
    print(f"    - With coordinator: {df['coordinator_name'].notna().sum():,}")
    print(f"    - With DOI: {df['grant_doi'].notna().sum():,}")

    if "framework_programme" in df.columns:
        print(f"\n  Framework Programmes:")
        print(df["framework_programme"].value_counts().to_string())

    if "funding_scheme" in df.columns:
        print(f"\n  Funding Schemes (top 15):")
        print(df["funding_scheme"].value_counts().head(15).to_string())

    # Calculate total funding
    total_funding = df["ec_max_contribution"].sum()
    print(f"\n  Total EC Contribution: EUR {total_funding/1e9:.2f} billion")

    return output_path

def build(directory):
    output = directory / "cordis.parquet"
    seen = set()
    with http_session() as session, pq.ParquetWriter(output, CORDIS_SCHEMA) as writer:
        for name, info in CORDIS_DATASETS.items():
            check_disk(directory)
            raw = directory / "programme.zip"
            download(session, info["projects_url"], raw)
            with zipfile.ZipFile(raw) as archive:
                members = archive.namelist()
                print(f"CORDIS {name} archive: {len(members)} members", flush=True)
                def member(wanted):
                    matches = [n for n in members if Path(n).name.lower() == wanted]
                    if len(matches) != 1:
                        raise ValueError(f"Expected one {wanted}, found {matches}")
                    return matches[0]
                def read_csv(wanted, required):
                    with archive.open(member(wanted)) as handle:
                        reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig"), delimiter=";")
                        if not required <= set(reader.fieldnames or []):
                            raise ValueError(f"Missing CORDIS fields in {wanted}")
                        return [{k: v if v != "" else None for k, v in row.items()} for row in reader]
                projects = read_csv("project.csv", {"id", "title", "objective", "fundingScheme",
                    "startDate", "endDate", "ecMaxContribution"})
                organizations = read_csv("organization.csv", {"projectID", "role", "name", "country"})
            if not projects or not organizations:
                raise ValueError(f"Empty programme or organizations: {name}")
            rows = process_projects(projects, organizations, info["framework"])
            frame = pd.DataFrame(rows)
            if frame.cordis_project_id.isna().any() or frame.cordis_project_id.astype(str).str.strip().eq("").any():
                raise ValueError("Invalid CORDIS project key")
            part = process_dataframe(frame, directory)
            table = pq.read_table(part)
            keep = []
            for key in table["cordis_project_id"].to_pylist():
                keep.append(key not in seen)
                seen.add(key)
            writer.write_table(table.filter(pa.array(keep)))
            check_disk(directory)
            raw.unlink()
            part.unlink()
            print(f"CORDIS {name}: {sum(keep)} unique rows", flush=True)
    return output


def main():
    run("european_commission", build, CORDIS_SCHEMA)


if __name__ == "__main__":
    main()
