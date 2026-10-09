"""CORDIS monthly bulk fetch; origin mapping with current per-project JSON adapter.

Preserve objective whitespace and structured identifiers. The CSV export is
not interchangeable: it removes line breaks and can shift quoted fields.
"""
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from award_fetch import download, http_session, run, check_disk
import re
import zipfile
from award_schemas import CORDIS_SCHEMA

CORDIS_DATASETS = {
    "horizon_europe": {
        "name": "Horizon Europe (2021-2027)",
        "projects_url": "https://cordis.europa.eu/data/cordis-HORIZONprojects-json.zip",
        "framework": "HORIZON",
    },
    "h2020": {
        "name": "Horizon 2020 (2014-2020)",
        "projects_url": "https://cordis.europa.eu/data/cordis-h2020projects-json.zip",
        "framework": "H2020",
    },
    "fp7": {
        "name": "FP7 (2007-2013)",
        "projects_url": "https://cordis.europa.eu/data/cordis-fp7projects-json.zip",
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
            df[col] = pd.to_numeric(df[col], errors="coerce")

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

def flatten_project(document):
    """Adapt CORDIS's current per-project JSON to the origin's flat inputs."""
    if document.get("contenttype") != "project" or not document.get("id"):
        raise ValueError("Not a CORDIS project document")
    # The old flat export represented missing scalar values as empty strings.
    project = {key: document.get(key, "") for key in (
        "id", "acronym", "rcn", "title", "objective", "status", "keywords",
        "startDate", "endDate", "ecSignatureDate", "totalCost",
        "ecMaxContribution", "contentUpdateDate")}
    project["grantDoi"] = document.get("identifiers", {}).get("grantDoi", "")
    relations = document.get("relations", {})
    associations = relations.get("associations", [])
    categories = relations.get("categories", [])
    def codes(items, attribute, value, field):
        values = [item.get(field, "").lstrip("/") for item in items
                  if item.get("attributes", {}).get(attribute) == value]
        return ";".join(value for value in values if value)
    project["fundingScheme"] = codes(categories, "classification", "projectFundingSchemeCategory", "code")
    project["topics"] = codes(associations, "type", "relatedTopic", "code")
    project["legalBasis"] = codes(associations, "type", "relatedLegalBasis", "code")
    project["masterCall"] = codes(associations, "type", "relatedMasterCall", "identifier")
    project["subCall"] = codes(associations, "type", "relatedSubCall", "identifier")
    organizations = []
    for org in associations:
        attrs = org.get("attributes", {})
        if org.get("contenttype") != "organization" or attrs.get("type") != "coordinator":
            continue
        address = org.get("address", {})
        organizations.append({
            "projectID": project["id"], "role": "coordinator",
            "name": org.get("legalName", ""), "shortName": org.get("shortName", ""),
            "country": address.get("country", ""), "city": address.get("city", ""),
            "organisationID": org.get("id", ""), "ecContribution": attrs.get("ecContribution", ""),
            "activityType": codes(org.get("relations", {}).get("categories", []),
                                  "classification", "organizationActivityType", "code"),
        })
    return project, organizations


def read_programme(archive, framework):
    members = archive.namelist()
    projects = [n for n in members if Path(n).name.lower() == "project.json"]
    organizations = [n for n in members if Path(n).name.lower() == "organization.json"]
    if len(projects) == len(organizations) == 1:
        with archive.open(projects[0]) as handle:
            records = json.load(handle)
        with archive.open(organizations[0]) as handle:
            orgs = json.load(handle)
        if not records or not orgs:
            raise ValueError("Empty CORDIS flat export")
        return process_projects(records, orgs, framework)
    documents = [n for n in members if re.fullmatch(r"project-rcn-\d+_en\.json", Path(n).name)]
    if not documents:
        raise ValueError("No CORDIS project records in archive")
    rows = []
    for name in documents:
        with archive.open(name) as handle:
            document = json.load(handle)
        project, orgs = flatten_project(document)
        rows.extend(process_projects([project], orgs, framework))
    return rows


def build(directory):
    output = directory / "cordis.parquet"
    seen = set()
    with http_session() as session, pq.ParquetWriter(output, CORDIS_SCHEMA) as writer:
        for name, info in CORDIS_DATASETS.items():
            check_disk(directory)
            raw = directory / "programme.zip"
            download(session, info["projects_url"], raw)
            with zipfile.ZipFile(raw) as archive:
                rows = read_programme(archive, info["framework"])
            if not rows:
                raise ValueError(f"Empty programme: {name}")
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
