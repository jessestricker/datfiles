import logging
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, assert_never
from urllib.parse import urljoin

import bs4
import requests
from pathvalidate import sanitize_filename
from requests.adapters import HTTPAdapter
from urllib3 import Retry

import datfile

_BASE_URL = "http://redump.org"

_logger = logging.getLogger(__name__)

_session = requests.Session()
_session_retry = Retry(
    total=5,
    status_forcelist={500, 502, 503, 504},
    backoff_factor=60,  # seconds
)
_session.mount("https://", HTTPAdapter(max_retries=_session_retry))
_session.mount("http://", HTTPAdapter(max_retries=_session_retry))


def scrape(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    _logger.info("fetching systems...")
    systems = _get_systems()

    datfile_count = sum(system.datfile_url is not None for system in systems)
    bios_datfile_count = sum(system.bios_datfile_url is not None for system in systems)
    _logger.info(
        "fetched %s systems with %s datfiles and %s bios datfiles",
        len(systems),
        datfile_count,
        bios_datfile_count,
    )

    _logger.info("downloading datfiles...")
    system_count = len(systems)

    for index, system in enumerate(systems):
        _logger.info(
            f"(%{len(str(system_count))}s/%s) %s",  # noqa: G004
            index + 1,
            len(systems),
            system.name,
        )

        if system.datfile_url is not None:
            _download_datfile(system.datfile_url, output_dir, ".xml")

        if system.bios_datfile_url is not None:
            _download_datfile(system.bios_datfile_url, output_dir, ".dat")

    _logger.info("downloaded datfiles")


@dataclass
class _System:
    name: str
    datfile_url: str | None
    bios_datfile_url: str | None


def _get_systems() -> list[_System]:
    systems: list[_System] = []

    resp = _session.get(urljoin(_BASE_URL, "downloads"))
    resp.raise_for_status()
    html = resp.text
    soup = bs4.BeautifulSoup(html, "lxml")

    table_rows = soup.css.select("#main table tr")
    header_tr, *body_trs = table_rows

    name_index, datfile_index, bios_datfile_index = _find_column_indices(header_tr)

    for tr in body_trs:
        tds = tr.find_all("td")

        name = tds[name_index].string
        if name is None:
            msg = "<td> for name is empty"
            raise ValueError(msg)

        datfile_url = _get_cell_link_url(tds[datfile_index])
        bios_datfile_url = _get_cell_link_url(tds[bios_datfile_index])

        system = _System(
            name=name,
            datfile_url=datfile_url,
            bios_datfile_url=bios_datfile_url,
        )
        systems.append(system)

    return systems


def _find_column_indices(tr: bs4.Tag) -> tuple[int, int, int]:
    systems_th = tr.find("th", text="Systems")
    if systems_th is None:
        msg = "<th>Systems</th> not found"
        raise ValueError(msg)

    datfiles_th = tr.find("th", text="Datfiles")
    if datfiles_th is None:
        msg = "<th>Datfiles</th> not found"
        raise ValueError(msg)

    bios_datfiles_th = tr.find("th", text="BIOS Datfiles")
    if bios_datfiles_th is None:
        msg = "<th>BIOS Datfiles</th> not found"
        raise ValueError(msg)

    return (
        tr.index(systems_th),
        tr.index(datfiles_th),
        tr.index(bios_datfiles_th),
    )


def _get_cell_link_url(td: bs4.Tag) -> str | None:
    link = td.find("a")
    if link is None:
        return None

    href = link["href"]
    if not isinstance(href, str):
        return None
    return href


def _download_datfile(
    url_path: str,
    output_dir: Path,
    target_ext: Literal[".xml", ".dat"],
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        # download
        downloaded_file = Path(tmpdir) / "download"
        _download_file(url_path, downloaded_file)

        # if archive, extract single file
        if zipfile.is_zipfile(downloaded_file):
            unzipped_file = Path(tmpdir) / "unzipped"
            _extract_zipped_file(downloaded_file, unzipped_file)
            downloaded_file = unzipped_file

        # parse datfile name from header
        if target_ext == ".xml":
            datfile_name = datfile.read_header_name_xml(downloaded_file)
        elif target_ext == ".dat":
            datfile_name = datfile.read_header_name_cmp(downloaded_file)
        else:
            assert_never(target_ext)

        if datfile_name is None:
            msg = "datfile does not contain a name"
            raise ValueError(msg)

        # copy datfile to output dir
        target_file = output_dir / (sanitize_filename(datfile_name) + target_ext)
        downloaded_file.copy(target_file)


def _download_file(remote_path: str, local_path: Path) -> None:
    url = urljoin(_BASE_URL, remote_path)
    with (
        local_path.open(mode="wb") as local_file,
        _session.get(url, stream=True) as resp,
    ):
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=512):
            local_file.write(chunk)


def _extract_zipped_file(path: Path, target_file: Path) -> None:
    with zipfile.ZipFile(path) as zip_file:
        entry_names = zip_file.namelist()
        if len(entry_names) != 1:
            msg = "zip file does not contain a single file"
            raise ValueError(msg)

        with zip_file.open(entry_names[0]) as src, target_file.open(mode="wb") as dst:
            shutil.copyfileobj(src, dst)
