""" General tools for working with wheels

Tools that aren't specific to delocation
"""

from __future__ import annotations

import csv
import glob
import hashlib
import logging
import os
from base64 import urlsafe_b64encode
from datetime import datetime, timezone
from itertools import product
from os.path import abspath, basename, dirname, exists
from os.path import join as pjoin
from os.path import relpath
from os.path import sep as psep
from os.path import splitext
from types import TracebackType
from typing import Generator, Iterable

from _vendor.wheel.pkginfo import read_pkg_info, write_pkg_info
from _vendor.wheel.wheelfile import WHEEL_INFO_RE
from tmpdirs import InTemporaryDirectory

from tools import dir2zip, unique_by_index, zip2dir

logger = logging.getLogger(__name__)


class WheelToolsError(Exception):
    pass


def _dist_info_dir(bdist_dir: str) -> str:
    """Get the .dist-info directory from an unpacked wheel

    Parameters
    ----------
    bdist_dir : str
        Path of unpacked wheel file
    """

    info_dirs = glob.glob(pjoin(bdist_dir, "*.dist-info"))
    if len(info_dirs) != 1:
        raise WheelToolsError("Should be exactly one `*.dist_info` directory")
    return info_dirs[0]


def rewrite_record(bdist_dir: str) -> None:
    """Rewrite RECORD file with hashes for all files in `wheel_sdir`

    Copied from :method:`wheel.bdist_wheel.bdist_wheel.write_record`

    Will also unsign wheel

    Parameters
    ----------
    bdist_dir : str
        Path of unpacked wheel file
    """
    info_dir = _dist_info_dir(bdist_dir)
    record_path = pjoin(info_dir, "RECORD")
    record_relpath = relpath(record_path, bdist_dir)
    # Unsign wheel - because we're invalidating the record hash
    sig_path = pjoin(info_dir, "RECORD.jws")
    if exists(sig_path):
        os.unlink(sig_path)

    def walk() -> Generator[str, None, None]:
        for dir, dirs, files in os.walk(bdist_dir):
            for f in files:
                yield pjoin(dir, f)

    def skip(path: str) -> bool:
        """Wheel hashes every possible file."""
        return path == record_relpath

    with open(record_path, "w+", newline="", encoding="utf-8") as record_file:
        writer = csv.writer(record_file)
        for path in walk():
            relative_path = relpath(path, bdist_dir)
            if skip(relative_path):
                hash_ = ""
                size = ""
            else:
                with open(path, "rb") as f:
                    data = f.read()
                digest = hashlib.sha256(data).digest()
                sha256 = urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
                hash_ = f"sha256={sha256}"
                size = f"{len(data)}"
            record_path = relpath(path, bdist_dir).replace(psep, "/")
            writer.writerow((record_path, hash_, size))


class InWheel(InTemporaryDirectory):
    """Context manager for doing things inside wheels

    On entering, you'll find yourself in the root tree of the wheel.  If you've
    asked for an output wheel, then on exit we'll rewrite the wheel record and
    pack stuff up for you.
    """

    def __init__(self, in_wheel: str, out_wheel: str | None = None) -> None:
        """Initialize in-wheel context manager

        Parameters
        ----------
        in_wheel : str
            filename of wheel to unpack and work inside
        out_wheel : None or str:
            filename of wheel to write after exiting.  If None, don't write and
            discard
        """
        self.in_wheel = abspath(in_wheel)
        self.out_wheel = None if out_wheel is None else abspath(out_wheel)
        super().__init__()

    def __enter__(self) -> str:
        zip2dir(self.in_wheel, self.name)
        return super().__enter__()

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self.out_wheel is not None:
            rewrite_record(self.name)
            date_time = None
            timestamp = os.environ.get("SOURCE_DATE_EPOCH")
            if timestamp:
                date_time = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
            dir2zip(self.name, self.out_wheel, date_time)
        return super().__exit__(exc, value, tb)


class InWheelCtx(InWheel):
    """Context manager for doing things inside wheels

    On entering, you'll find yourself in the root tree of the wheel.  If you've
    asked for an output wheel, then on exit we'll rewrite the wheel record and
    pack stuff up for you.

    The context manager returns itself from the __enter__ method, so you can
    set things like ``out_wheel``.  This is useful when processing in the wheel
    will dicate what the output wheel name is, or whether you want to save at
    all.

    The current path of the wheel contents is set in the attribute
    ``wheel_path``.
    """

    def __init__(self, in_wheel: str, out_wheel: str | None = None) -> None:
        """Init in-wheel context manager returning self from enter

        Parameters
        ----------
        in_wheel : str
            filename of wheel to unpack and work inside
        out_wheel : None or str:
            filename of wheel to write after exiting.  If None, don't write and
            discard
        """
        super().__init__(in_wheel, out_wheel)
        self.path = None

    def __enter__(self):
        self.path = super().__enter__()
        return self

    def iter_files(self) -> Generator[str, None, None]:
        if self.path is None:
            raise ValueError("This function should be called from context manager")
        record_names = glob.glob(os.path.join(self.path, "*.dist-info/RECORD"))
        if len(record_names) != 1:
            raise ValueError("Should be exactly one `*.dist_info` directory")

        with open(record_names[0]) as f:
            record = f.read()
        reader = csv.reader(r for r in record.splitlines())
        for row in reader:
            filename = row[0]
            yield filename


def add_platforms(wheel_ctx: InWheelCtx, platforms: list[str], remove_platforms: Iterable[str] = ()) -> str:
    """Add platform tags `platforms` to a wheel

    Add any platform tags in `platforms` that are missing
    to wheel_ctx's filename and ``WHEEL`` file.

    Parameters
    ----------
    wheel_ctx : InWheelCtx
        An open wheel context
    platforms : list
        platform tags to add to wheel filename and WHEEL tags - e.g.
        ``('macosx_10_9_intel', 'macosx_10_9_x86_64')
    remove_platforms : iterable
        platform tags to remove to the wheel filename and WHEEL tags, e.g.
        ``('linux_x86_64',)`` when ``('manylinux_x86_64')`` is added
    """
    definitely_not_purelib = False

    if wheel_ctx.path is None:
        raise ValueError("This function should be called from wheel_ctx context manager")

    info_fname = pjoin(_dist_info_dir(wheel_ctx.path), "WHEEL")
    info = read_pkg_info(info_fname)
    # Check what tags we have
    if wheel_ctx.out_wheel is not None:
        out_dir = dirname(wheel_ctx.out_wheel)
        wheel_fname = basename(wheel_ctx.out_wheel)
    else:
        out_dir = "."
        wheel_fname = basename(wheel_ctx.in_wheel)

    parsed_fname = WHEEL_INFO_RE.match(wheel_fname)
    fparts = parsed_fname.groupdict()
    original_fname_tags = fparts["plat"].split(".")
    logger.info("Previous filename tags: %s", ", ".join(original_fname_tags))
    fname_tags = [tag for tag in original_fname_tags if tag not in remove_platforms]
    fname_tags = unique_by_index(fname_tags + platforms)

    # Can't be 'any' and another platform
    if "any" in fname_tags and len(fname_tags) > 1:
        fname_tags.remove("any")
        remove_platforms.append("any")
        definitely_not_purelib = True

    if fname_tags != original_fname_tags:
        logger.info("New filename tags: %s", ", ".join(fname_tags))
    else:
        logger.info("No filename tags change needed.")

    _, ext = splitext(wheel_fname)
    fparts["plat"] = ".".join(fname_tags)
    fparts["ext"] = ext
    out_wheel_fname = "{namever}-{pyver}-{abi}-{plat}{ext}".format(**fparts)
    out_wheel = pjoin(out_dir, out_wheel_fname)

    in_info_tags = [tag for name, tag in info.items() if name == "Tag"]
    logger.info("Previous WHEEL info tags: %s", ", ".join(in_info_tags))
    # Python version, C-API version combinations
    pyc_apis = ["-".join(tag.split("-")[:2]) for tag in in_info_tags]
    # unique Python version, C-API version combinations
    pyc_apis = unique_by_index(pyc_apis)
    # Add new platform tags for each Python version, C-API combination
    wanted_tags = ["-".join(tup) for tup in product(pyc_apis, platforms)]
    new_tags = [tag for tag in wanted_tags if tag not in in_info_tags]
    unwanted_tags = ["-".join(tup) for tup in product(pyc_apis, remove_platforms)]
    updated_tags = [tag for tag in in_info_tags if tag not in unwanted_tags]
    updated_tags += new_tags
    if updated_tags != in_info_tags:
        del info["Tag"]
        for tag in updated_tags:
            info.add_header("Tag", tag)

        if definitely_not_purelib:
            info["Root-Is-Purelib"] = "False"
            logger.info("Changed wheel type to Platlib")

        logger.info("New WHEEL info tags: %s", ", ".join(info.get_all("Tag")))
        write_pkg_info(info_fname, info)
    else:
        logger.info("No WHEEL info change needed.")
    return out_wheel
