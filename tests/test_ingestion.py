import zipfile
from datetime import date

import pytest

from cnpj_pipeline.ingestion import release as release_api
from cnpj_pipeline.ingestion.bronze import InvalidSourceArchiveError, extract_single_member
from cnpj_pipeline.ingestion.download import download_file
from cnpj_pipeline.schemas import CNAES, EMPRESAS

LISTING = """
<tr><td><a href="/">Parent Directory</a></td></tr>
<tr><td><a href="2026-09-14/">2026-09-14/</a></td></tr>
<tr><td><a href="2026-10-13/">2026-10-13/</a></td></tr>
<tr><td><a href="2023-03-15/">2023-03-15/</a></td></tr>
<tr><td><a href="README.txt">README.txt</a></td></tr>
"""


class FakeResponse:
    def __init__(self, text="", content=b"", headers=None, status=200):
        self.text = text
        self._content = content
        self.headers = headers or {}
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(self.status)

    def iter_content(self, chunk_size):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, listing="", payload=b"", advertised_size=None):
        self.listing = listing
        self.payload = payload
        self.advertised_size = len(payload) if advertised_size is None else advertised_size
        self.get_calls = 0

    def head(self, url, **kwargs):
        return FakeResponse(headers={"Content-Length": str(self.advertised_size)})

    def get(self, url, **kwargs):
        self.get_calls += 1
        if kwargs.get("stream"):
            return FakeResponse(content=self.payload)
        return FakeResponse(text=self.listing)


def test_parse_release_dates_sorted_and_ignores_other_links():
    assert release_api.parse_release_dates(LISTING) == [
        date(2023, 3, 15),
        date(2026, 9, 14),
        date(2026, 10, 13),
    ]


def test_resolve_release_defaults_to_latest():
    assert release_api.resolve_release(FakeSession(LISTING), "https://x/") == date(2026, 10, 13)


def test_resolve_release_validates_requested_release():
    session = FakeSession(LISTING)
    assert release_api.resolve_release(session, "https://x/", "2026-09-14") == date(2026, 9, 14)
    with pytest.raises(release_api.ReleaseNotFoundError):
        release_api.resolve_release(session, "https://x/", "2026-01-01")


def test_source_files_numbered_and_single():
    release = date(2026, 10, 13)
    numbered = release_api.source_files(EMPRESAS, "https://x/arquivos", release, (0, 7))
    assert numbered == [
        (0, "Empresas0.zip", "https://x/arquivos/2026-10-13/Empresas0.zip"),
        (7, "Empresas7.zip", "https://x/arquivos/2026-10-13/Empresas7.zip"),
    ]
    single = release_api.source_files(CNAES, "https://x/arquivos/", release, (0, 7))
    assert single == [(None, "Cnaes.zip", "https://x/arquivos/2026-10-13/Cnaes.zip")]


def test_download_writes_file_and_skips_when_already_complete(tmp_path):
    session = FakeSession(payload=b"x" * 1000)
    target = tmp_path / "Empresas0.zip"

    download_file(session, "https://x/Empresas0.zip", target, chunk_bytes=128)
    download_file(session, "https://x/Empresas0.zip", target, chunk_bytes=128)

    assert target.read_bytes() == b"x" * 1000
    assert session.get_calls == 1
    assert not (tmp_path / "Empresas0.zip.part").exists()


def test_download_rejects_truncated_payload(tmp_path):
    session = FakeSession(payload=b"x" * 10, advertised_size=500)
    target = tmp_path / "Socios0.zip"
    with pytest.raises(IOError):
        download_file(session, "https://x/Socios0.zip", target)
    assert not target.exists()


def test_extract_single_member_streams_bytes_untouched(tmp_path):
    raw = '"00000000";"BANCO DO BRASIL S/A";"2038"\n'.encode("iso-8859-1")
    zip_path = tmp_path / "Empresas0.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("K3241.K03200Y0.D51011.EMPRECSV", raw)

    target = tmp_path / "bronze" / "empresas_20261013_0.csv"
    extract_single_member(zip_path, target)

    assert target.read_bytes() == raw


def test_extract_rejects_multi_member_and_corrupt_archives(tmp_path):
    multi = tmp_path / "multi.zip"
    with zipfile.ZipFile(multi, "w") as archive:
        archive.writestr("a.csv", "1")
        archive.writestr("b.csv", "2")
    with pytest.raises(InvalidSourceArchiveError):
        extract_single_member(multi, tmp_path / "out.csv")

    broken = tmp_path / "broken.zip"
    broken.write_bytes(b"not a zip")
    with pytest.raises(InvalidSourceArchiveError):
        extract_single_member(broken, tmp_path / "out.csv")
