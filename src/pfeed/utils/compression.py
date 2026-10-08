"""Byte-level compression/decompression of raw data (e.g. downloaded vendor .gz/.zip/.tar files)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

from enum import StrEnum


class Compression(StrEnum):
    GZIP = "gzip"
    ZSTD = "zstd"
    BZ2 = "bz2"
    XZ = "xz"
    ZIP = "zip"


_MAGIC_BYTES: dict[Compression, bytes] = {
    Compression.GZIP: b"\x1f\x8b",
    Compression.ZSTD: b"\x28\xb5\x2f\xfd",
    Compression.BZ2: b"BZh",
    Compression.XZ: b"\xfd7zXZ\x00",
    Compression.ZIP: b"PK\x03\x04",
}


def _compress_gzip(data: bytes, compression_level: int = 9) -> bytes:
    import gzip

    return gzip.compress(data, compresslevel=compression_level)


def _decompress_gzip(data: bytes) -> bytes:
    import gzip

    return gzip.decompress(data)


def _compress_zstd(data: bytes) -> bytes:
    import pyarrow as pa

    buffer = pa.BufferOutputStream()
    with pa.CompressedOutputStream(buffer, "zstd") as stream:
        stream.write(data)
    return buffer.getvalue().to_pybytes()


def _decompress_zstd(data: bytes) -> bytes:
    import pyarrow as pa

    return pa.input_stream(pa.py_buffer(data), compression="zstd").read()


def _compress_bz2(data: bytes, compression_level: int = 9) -> bytes:
    import bz2

    return bz2.compress(data, compresslevel=compression_level)


def _decompress_bz2(data: bytes) -> bytes:
    import bz2

    return bz2.decompress(data)


def _compress_xz(data: bytes, compression_level: int = 6) -> bytes:
    import lzma

    return lzma.compress(data, preset=compression_level)


def _decompress_xz(data: bytes) -> bytes:
    import lzma

    return lzma.decompress(data)


def _compress_zip(
    data: bytes, filename: str = "file.txt", compression_level: int = 6
) -> bytes:
    import io
    import zipfile

    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=compression_level
    ) as zf:
        zf.writestr(filename, data)
    return buffer.getvalue()


def _decompress_zip(data: bytes) -> bytes:
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return zf.read(zf.namelist()[0])  # Assumes single file in ZIP


_COMPRESSORS: dict[Compression, Callable[..., bytes]] = {
    Compression.GZIP: _compress_gzip,
    Compression.ZSTD: _compress_zstd,
    Compression.BZ2: _compress_bz2,
    Compression.XZ: _compress_xz,
    Compression.ZIP: _compress_zip,
}


_DECOMPRESSORS: dict[Compression, Callable[[bytes], bytes]] = {
    Compression.GZIP: _decompress_gzip,
    Compression.ZSTD: _decompress_zstd,
    Compression.BZ2: _decompress_bz2,
    Compression.XZ: _decompress_xz,
    Compression.ZIP: _decompress_zip,
}


def _is_tar(data: bytes) -> bool:
    import io
    import tarfile

    return tarfile.is_tarfile(io.BytesIO(data))


def _extract_tar(data: bytes) -> bytes:
    """Extract first file from TAR archive."""
    import io
    import tarfile

    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tf:
        first_file = next(f for f in tf if f.isfile())
        fileobj = tf.extractfile(first_file)
        assert fileobj is not None, f"cannot extract {first_file.name} from TAR"
        return fileobj.read()


def detect(data: bytes) -> Compression | None:
    """Detect compression format from magic bytes, None if not compressed (or unknown)."""
    for compression, magic in _MAGIC_BYTES.items():
        if data.startswith(magic):
            return compression
    return None


def compress(data: bytes, compression: Compression | str, **kwargs) -> bytes:
    """Compress data with the given format; kwargs (e.g. compression_level) go to the format's compressor."""
    return _COMPRESSORS[Compression(compression)](data, **kwargs)


def decompress(data: bytes) -> bytes:
    """Recursively decompress data, handling nested compression and TAR archives.

    Returns data as-is if no compression is detected.
    """
    if compression := detect(data):
        return decompress(_DECOMPRESSORS[compression](data))
    # TAR is an archive format, not compression
    elif _is_tar(data):
        return decompress(_extract_tar(data))
    else:
        return data
