import logging
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import bs4
from pathvalidate import sanitize_filename
from requests import Request

import datfile
from requests_util import Session

if TYPE_CHECKING:
    from requests import Response


_logger = logging.getLogger(__name__)

_session = Session(
    timeout=3 * 60,  # seconds
)


def scrape(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # get prepare page
    _logger.info("get prepare page")
    prepare_page = _session.get(
        url="https://datomatic.no-intro.org/index.php?page=download&op=daily",
    )
    prepare_page.raise_for_status()

    download_page_request = _get_next_request(prepare_page)

    # get download page
    _logger.info("get download page")
    download_page = _session.send(_session.prepare_request(download_page_request))
    download_page.raise_for_status()

    download_request = _get_next_request(download_page)

    # download datfiles
    _logger.info("download datfiles")
    with TemporaryDirectory() as tmpdir:
        download_file = Path(tmpdir) / "download.zip"
        _session.download_content(download_request, download_file)

        _logger.info("extracting datfiles")
        _extract_downloaded_archive(download_file, output_dir)


def _get_next_request(resp: Response) -> Request:
    html = bs4.BeautifulSoup(resp.text, "html5lib")
    form = html.select_one("#content form")
    if form is None:
        msg = "can not find <form>"
        raise ValueError(msg)

    action = str(form.attrs["action"])
    method = str(form.attrs["method"])
    inputs = {
        str(element.attrs["name"]): str(element.attrs["value"])
        for element in form.select("input[type=submit], input[checked]")
    }

    return Request(
        method=method,
        url=urljoin(resp.url, action),
        data=inputs,
    )


def _extract_downloaded_archive(archive: Path, output_dir: Path) -> None:
    if not zipfile.is_zipfile(archive):
        msg = "downloaded file is not a zip archive"
        raise ValueError(msg)

    with TemporaryDirectory() as temp_dir:
        # extract all members in "No-Intro/" to temp dir
        with zipfile.ZipFile(archive) as zip_file:
            for member in zip_file.infolist():
                if not member.is_dir() and member.filename.startswith("No-Intro/"):
                    zip_file.extract(member, temp_dir)

        # copy extracted files from temp dir to output dir, with canonical name
        for extracted_file in Path(temp_dir).rglob("*"):
            if extracted_file.is_dir():
                continue

            canonical_name = datfile.read_canonical_name(extracted_file, "xml")
            output_file = (output_dir / sanitize_filename(canonical_name)).with_suffix(
                ".xml"
            )
            extracted_file.copy(output_file)
