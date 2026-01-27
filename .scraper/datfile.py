import xml.etree.ElementTree as ET
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Literal, assert_never

if TYPE_CHECKING:
    from pathlib import Path


type DatfileFormat = Literal["xml", "cmp"]


def read_canonical_name(path: Path, fmt: DatfileFormat) -> str:
    match fmt:
        case "xml":
            name = read_header_name_xml(path)
        case "cmp":
            name = read_header_name_cmp(path)
        case _:
            assert_never(fmt)

    if name is None:
        msg = "datfile does not contain header with name"
        raise ValueError(msg)

    return name


def read_header_name_xml(path: Path) -> str | None:
    scope: list[str] = []
    parser = ET.iterparse(path, events=("start", "end"))  # noqa: S314

    try:
        for event, elem in parser:
            if event == "start":
                scope.append(elem.tag)
            elif event == "end":
                scope.pop()

            # look for end tag of /datafile/header/name
            if (
                event == "end"
                and elem.tag == "name"
                and scope == ["datafile", "header"]
            ):
                return elem.text

        return None

    finally:
        parser.close()


def read_header_name_cmp(path: Path) -> str | None:
    scope: list[str] = []
    parser = _parse_clrmamepro(path)

    for event in parser:
        match event[0]:
            case "open":
                name = event[1]
                scope.append(name)
            case "close":
                scope.pop()
            case "value":
                name, value = event[1:]
                if name == "name" and scope == ["clrmamepro"]:
                    return value

    return None


type _ParseEvent = (
    tuple[Literal["close"]]
    | tuple[Literal["open"], str]
    | tuple[Literal["value"], str, str]
)


def _parse_clrmamepro(path: Path) -> Generator[_ParseEvent]:
    text = path.read_text()

    while True:
        # skip space, stop if no remaining text
        text = _skip_while(text, str.isspace)
        if not text:
            break

        # check for close event
        if text[0] == ")":
            text = text[1:]
            yield ("close",)
            continue

        # parse name, skip following space
        name, text = _take_until(text, str.isspace)
        text = _skip_while(text, str.isspace)

        # check for open event
        if text[0] == "(":
            text = text[1:]  # skip opening parenthesis
            yield "open", name
            continue

        # check for quoted value event
        if text[0] == '"':
            text = text[1:]  # skip opening double quote
            value, text = _take_until(text, lambda char: char == '"')
            text = text[1:]  # skip closing double quote
            yield "value", name, value
            continue

        # unquoted value event
        value, text = _take_until(text, str.isspace)
        yield "value", name, value


type _StrPredicate = Callable[[str], bool]


def _find(text: str, predicate: _StrPredicate) -> int | None:
    return next(
        (index for index, char in enumerate(text) if predicate(char)),
        None,
    )


def _take_until(text: str, predicate: _StrPredicate) -> tuple[str, str]:
    end = _find(text, predicate)
    if end is None:
        return text, ""
    return text[:end], text[end:]


def _skip_while(text: str, predicate: _StrPredicate) -> str:
    _, tail = _take_until(text, lambda char: not predicate(char))
    return tail
