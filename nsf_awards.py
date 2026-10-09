"""Fetch the full NSF bulk release; run with python -m nsf_awards."""
import json
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from award_fetch import download, http_session, run
from award_schemas import NSF_SCHEMA

NSF_API_URL = "https://api.nsf.gov/services/v2/s3/list-files"


def flatten_award(award: dict) -> dict:
    """
    Flatten nested structures in an award record for parquet storage.

    Handles nested fields like inst, pi, pgm_ele, pgm_ref, perf_inst, app_fund, por.

    Args:
        award: Raw award dict from JSON

    Returns:
        Flattened award dict
    """
    flat = {}

    simple_fields = [
        'awd_id', 'agcy_id', 'tran_type', 'awd_istr_txt', 'awd_titl_txt',
        'cfda_num', 'org_code', 'po_phone', 'po_email', 'po_sign_block_name',
        'awd_eff_date', 'awd_exp_date', 'tot_intn_awd_amt', 'awd_amount',
        'awd_min_amd_letter_date', 'awd_max_amd_letter_date',
        'awd_abstract_narration', 'awd_arra_amount', 'awd_agcy_code',
        'fund_agcy_code', 'dir_abbr', 'div_abbr', 'org_dir_long_name',
        'org_div_long_name', 'oblg_fy', '_source_file'
    ]

    for field in simple_fields:
        flat[field] = award.get(field)

    flat['inst_name'] = None
    flat['inst_city'] = None
    flat['inst_state_code'] = None
    flat['inst_zip'] = None
    flat['inst_country_code'] = None
    flat['inst_country_name'] = None
    flat['perf_inst_count'] = 0
    flat['perf_inst_all'] = None

    perf_inst = award.get('perf_inst')
    if perf_inst:
        if isinstance(perf_inst, dict):
            flat['inst_name'] = perf_inst.get('perf_inst_name')
            flat['inst_city'] = perf_inst.get('perf_city_name')
            flat['inst_state_code'] = perf_inst.get('perf_st_code')
            flat['inst_zip'] = perf_inst.get('perf_zip_code')
            flat['inst_country_code'] = perf_inst.get('perf_ctry_code')
            flat['inst_country_name'] = perf_inst.get('perf_ctry_name')
            flat['perf_inst_count'] = 1
        elif isinstance(perf_inst, list) and perf_inst:
            first_perf = perf_inst[0]
            flat['inst_name'] = first_perf.get('perf_inst_name')
            flat['inst_city'] = first_perf.get('perf_city_name')
            flat['inst_state_code'] = first_perf.get('perf_st_code')
            flat['inst_zip'] = first_perf.get('perf_zip_code')
            flat['inst_country_code'] = first_perf.get('perf_ctry_code')
            flat['inst_country_name'] = first_perf.get('perf_ctry_name')
            flat['perf_inst_count'] = len(perf_inst)
            if len(perf_inst) > 1:
                flat['perf_inst_all'] = json.dumps(perf_inst)

    flat['pi_full_name'] = None
    flat['pi_first_name'] = None
    flat['pi_last_name'] = None
    flat['pi_email'] = None
    flat['pi_count'] = 0
    flat['pi_all'] = None

    pi_list = award.get('pi', [])
    if isinstance(pi_list, list) and pi_list:
        first_pi = pi_list[0]
        flat['pi_full_name'] = first_pi.get('pi_full_name')
        flat['pi_first_name'] = first_pi.get('pi_first_name')
        flat['pi_last_name'] = first_pi.get('pi_last_name')
        flat['pi_email'] = first_pi.get('pi_email')

        flat['pi_count'] = len(pi_list)
        if len(pi_list) > 1:
            flat['pi_all'] = json.dumps(pi_list)

    pgm_ele = award.get('pgm_ele', [])
    if isinstance(pgm_ele, list) and pgm_ele:
        flat['pgm_ele_codes'] = ','.join(str(p.get('pgm_ele_code', '')) for p in pgm_ele)
        flat['pgm_ele_text'] = '; '.join(str(p.get('pgm_ele_txt', '')) for p in pgm_ele)
        flat['pgm_ele_all'] = json.dumps(pgm_ele)

    pgm_ref = award.get('pgm_ref', [])
    if isinstance(pgm_ref, list) and pgm_ref:
        flat['pgm_ref_codes'] = ','.join(str(p.get('pgm_ref_code', '')) for p in pgm_ref)
        flat['pgm_ref_text'] = '; '.join(str(p.get('pgm_ref_txt', '')) for p in pgm_ref)
        flat['pgm_ref_all'] = json.dumps(pgm_ref)

    app_fund = award.get('app_fund', [])
    if isinstance(app_fund, list) and app_fund:
        flat['app_fund_all'] = json.dumps(app_fund)

    por = award.get('por', {})
    if isinstance(por, dict):
        flat['por_email'] = por.get('por_email')
        flat['por_name'] = por.get('por_name')

    return flat


def build(directory):
    output = directory / "nsf.parquet"
    with http_session() as session:
        response = session.get(NSF_API_URL, timeout=(15, 120))
        response.raise_for_status()
        manifest = response.json()
        if manifest.get("isTruncated"):
            raise ValueError("NSF file list is truncated")
        files = sorted((f for f in manifest["files"]
                        if f["fileName"].endswith(".zip")), key=lambda f: f["fileName"])
        if not files or len({f["fileName"] for f in files}) != len(files):
            raise ValueError("Empty or duplicate NSF archive list")
        total = 0
        with pq.ParquetWriter(output, NSF_SCHEMA, compression="snappy") as writer:
            for info in files:
                raw = directory / "download.zip"
                download(session, info["downloadUrl"], raw, expected_size=info["size"])
                count = 0
                batch = []
                with zipfile.ZipFile(raw) as archive:
                    for member in archive.infolist():
                        if not member.filename.endswith(".json"):
                            continue
                        with archive.open(member) as handle:
                            award = json.load(handle)
                        award["_source_file"] = info["fileName"]
                        flat = flatten_award(award)
                        # The laptop output is scalar strings, double amounts and integer counts.
                        for field in NSF_SCHEMA:
                            value = flat.get(field.name)
                            if value is not None:
                                if pa.types.is_string(field.type):
                                    value = str(value)
                                elif pa.types.is_floating(field.type):
                                    value = float(value)
                                elif pa.types.is_null(field.type):
                                    raise ValueError(f"Previously null NSF column populated: {field.name}")
                            flat[field.name] = value
                        if not flat["awd_id"]:
                            raise ValueError(f"Missing NSF award id in {info['fileName']}")
                        batch.append(flat)
                        count += 1
                        if len(batch) == 1000:
                            writer.write_table(pa.Table.from_pylist(batch, schema=NSF_SCHEMA))
                            batch.clear()
                    if batch:
                        writer.write_table(pa.Table.from_pylist(batch, schema=NSF_SCHEMA))
                if not count:
                    raise ValueError(f"No JSON awards in {info['fileName']}")
                # Each archive's records are now in parquet; no extracted files or raw cache.
                raw.unlink()
                total += count
                print(json.dumps({"archive": info["fileName"], "rows": count,
                                  "total_rows": total}), flush=True)
    return output


if __name__ == "__main__":
    run("nsf", build, NSF_SCHEMA)
