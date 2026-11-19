"""nva.py: NVA search hits become the oai_dc records Repo.py reads (oxjob #2380). Offline; hits shaped like the API's."""
from datetime import date
import xml.etree.ElementTree as ET

import pytest

import nva

OAI = f"{{{nva.OAI_NS}}}"
DC = f"{{{nva.DC_NS}}}"


def hit(**over):
    h = {
        "id": "https://api.nva.unit.no/publication/01a1f3e5aa28-2061a52a-be6a-43f4-a318-67e55baac5e9",
        "identifier": "01a1f3e5aa28-2061a52a-be6a-43f4-a318-67e55baac5e9",
        "modifiedDate": "2026-11-19T13:51:37.505150778Z",
        "createdDate": "2026-11-19T13:01:17.224911050Z",
        "additionalIdentifiers": [
            {"sourceName": "nva@null", "type": "CristinIdentifier", "value": "10528900"},
            {"sourceName": "nva@sikt", "type": "HandleIdentifier", "value": "https://hdl.handle.net/11250/5600377"},
            {"sourceName": "brage@ntnu", "type": "HandleIdentifier", "value": "https://hdl.handle.net/11250/228842"},
        ],
        "topLevelOrganizations": [{"id": "https://api.nva.unit.no/cristin/organization/209.0.0.0",
                                   "labels": {"nb": "Universitetet i Innlandet", "en": "University of Inland Norway"}}],
        "associatedArtifacts": [{"type": "OpenFile", "license": {"type": "License",
                                                                  "value": "https://nva.sikt.no/license/copyright-act/1.0"}}],
        "entityDescription": {
            "mainTitle": "Genomic monitoring of Atlantic halibut",
            "abstract": "We genotyped 2,000 fish.",
            "language": "http://lexvo.org/id/iso639-3/eng",
            "tags": ["aquaculture genetics"],
            "publicationDate": {"year": "2026", "month": "12", "day": "3", "type": "PublicationDate"},
            "reference": {"type": "Reference", "publicationInstance": {"type": "DegreePhd"}},
            # the API sends sequence as a string; "10" must sort after "2"
            "contributors": [
                {"role": {"type": "Supervisor"}, "identity": {"name": "Elisabeth Kommisrud"}, "sequence": "2"},
                {"role": {"type": "Creator"}, "identity": {"name": "Second Author"}, "sequence": "10"},
                {"role": {"type": "Creator"}, "identity": {"name": "Valentina Krivenjeva Sinani"}, "sequence": "1"},
                {"role": {"type": "Supervisor"}, "identity": {"name": "Marte Wetten"}, "sequence": "4"},
            ],
        },
    }
    h.update(over)
    return h


def parse(h):
    rec = ET.fromstring(nva.to_oai_record(h))
    dc = rec.find(f"{OAI}metadata/{{{nva.OAI_DC_NS}}}dc")
    return rec, {t: [e.text for e in dc.findall(f"{DC}{t}")] for t in
                 ("title", "creator", "contributor", "type", "identifier", "rights", "date", "language", "publisher",
                  "description", "subject")}


def test_header_matches_nva_oai_feed():
    rec, _ = parse(hit())
    assert rec.find(f"{OAI}header/{OAI}identifier").text == hit()["id"]
    assert rec.find(f"{OAI}header/{OAI}datestamp").text == "2026-11-19T13:51:37Z"
    assert rec.find(f"{OAI}header/{OAI}setSpec").text == "resourceTypeGeneral:DegreePhd"


def test_authors_are_creators_in_byline_order_and_never_supervisors():
    _, dc = parse(hit())
    assert dc["creator"] == ["Valentina Krivenjeva Sinani", "Second Author"]
    assert dc["contributor"] == []


def test_type_maps_to_eu_repo_and_keeps_nva_type():
    _, dc = parse(hit())
    assert dc["type"] == ["info:eu-repo/semantics/doctoralThesis", "DegreePhd"]


def test_landing_page_is_the_brage_handle_first():
    _, dc = parse(hit())
    assert dc["identifier"][0] == "https://hdl.handle.net/11250/228842"
    assert "https://nva.sikt.no/registration/01a1f3e5aa28-2061a52a-be6a-43f4-a318-67e55baac5e9" in dc["identifier"]
    assert "CRISTIN:10528900" in dc["identifier"]


def test_open_file_without_cc_licence_is_open_access():
    _, dc = parse(hit())
    assert dc["rights"] == ["info:eu-repo/semantics/openAccess"]


def test_cc_licence_comes_first():
    h = hit(associatedArtifacts=[{"type": "OpenFile", "license": {
        "value": "https://creativecommons.org/licenses/by/4.0/"}}])
    assert parse(h)[1]["rights"] == ["https://creativecommons.org/licenses/by/4.0/", "info:eu-repo/semantics/openAccess"]


@pytest.mark.parametrize("arts", [[], [{"type": "HiddenFile", "license": None}],
                                  [{"type": "PendingOpenFile", "license": {
                                      "value": "https://creativecommons.org/licenses/by/4.0/"}}]])
def test_no_public_file_says_nothing_about_rights(arts):
    assert parse(hit(associatedArtifacts=arts))[1]["rights"] == []


def test_other_fields():
    _, dc = parse(hit())
    assert dc["date"] == ["2026-12-03"]
    assert dc["language"] == ["eng"]  # Repo.py normalises ISO 639-3
    assert dc["publisher"] == ["University of Inland Norway"]
    assert dc["description"] == ["We genotyped 2,000 fish."]
    assert dc["subject"] == ["aquaculture genetics"]


def test_year_only_date_and_doi():
    h = hit()
    h["entityDescription"]["publicationDate"] = {"year": "2004"}
    h["entityDescription"]["reference"]["doi"] = "https://doi.org/10.1234/x"
    _, dc = parse(h)
    assert dc["date"] == ["2004"]
    assert "https://doi.org/10.1234/x" in dc["identifier"]


def test_record_survives_harvester_serialisation():
    """save_batch re-parses record.raw and appends it to <oai_records>: the file must read like any OAI harvest."""
    import defusedxml.ElementTree as DefusedET
    root = ET.Element("oai_records")
    root.append(DefusedET.fromstring(nva.nva_record(hit()).raw))
    xml = ET.tostring(root, encoding="unicode")
    assert "<ns0:record>" in xml and "<ns1:dc>" in xml and "<dc:creator>" in xml


def test_characters_xml_forbids_are_dropped_so_the_record_still_parses():
    """An abstract with a pasted U+0002 stopped the first NVA harvest: save_batch could not parse the record."""
    import defusedxml.ElementTree as DefusedET
    h = hit()
    h["entityDescription"]["abstract"] = "We genotyped\x02 2,000\x1f fish.￾"
    h["entityDescription"]["mainTitle"] = "Genomic\x0e monitoring\tof Atlantic halibut"
    elem = DefusedET.fromstring(nva.nva_record(h).raw)
    assert elem.find(f".//{DC}description").text == "We genotyped 2,000 fish."
    assert elem.find(f".//{DC}title").text == "Genomic monitoring\tof Atlantic halibut"  # tab is legal XML


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload, self.status_code, self.text = payload, status, ""

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, pages):
        self.pages, self.calls, self.headers = list(pages), [], {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return FakeResponse(self.pages.pop(0))


def test_window_pages_by_search_after():
    s = FakeSession([{"totalHits": 2, "hits": [hit()], "nextSearchAfterResults": "https://next"},
                     {"hits": [hit(id="https://api.nva.unit.no/publication/b")], "nextSearchAfterResults": "https://n2"},
                     {"hits": []}])
    recs = nva.NvaRecords("DegreePhd,DegreeMaster", first=date(2026, 11, 18), last=date(2026, 11, 19), session=s)
    assert recs.total == 2
    assert [r.header.identifier for r in recs][-1] == "https://api.nva.unit.no/publication/b"
    params = s.calls[0][1]
    assert params["modified_since"] == "2026-11-18" and params["modified_before"] == "2026-11-20"
    assert params["sort"] == "modified_date:asc"


def test_empty_window_is_no_records_match():
    with pytest.raises(nva.NoRecordsMatch):
        nva.NvaRecords("DegreePhd", first=date(2026, 11, 18), last=date(2026, 11, 18),
                       session=FakeSession([{"totalHits": 0, "hits": []}]))


def test_a_record_that_will_not_convert_is_skipped_not_the_end_of_the_feed():
    bad = hit()
    del bad["id"]
    s = FakeSession([{"totalHits": 2, "hits": [bad, hit()], "nextSearchAfterResults": None}])
    assert [r.header.identifier for r in nva.NvaRecords("DegreePhd", session=s)] == [hit()["id"]]


def test_deadline_raises_the_callers_error():
    class Deadline(Exception):
        pass
    with pytest.raises(Deadline):
        nva.NvaRecords("DegreePhd", deadline=0, deadline_error=Deadline, session=FakeSession([]))
