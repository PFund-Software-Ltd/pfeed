from enum import StrEnum


# Compression functions
def _compress_gzip(data: bytes, compression_level: int = 9) -> bytes:
    import gzip
    return gzip.compress(data, compresslevel=compression_level)


def _decompress_gzip(data: bytes) -> bytes:
    import gzip
    return gzip.decompress(data)


def _compress_zstd(data: bytes) -> bytes:
    import zstandard as zstd
    cctx = zstd.ZstdCompressor()
    return cctx.compress(data)


def _decompress_zstd(data: bytes) -> bytes:
    import zstandard as zstd
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(data)


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


def _compress_zip(data: bytes, filename: str = "file.txt", compression_level: int = 6) -> bytes:
    import zipfile
    import io
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=compression_level) as zf:
        zf.writestr(filename, data)
    return buffer.getvalue()


def _decompress_zip(data: bytes) -> bytes:
    import zipfile
    import io
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return zf.read(zf.namelist()[0])  # Assumes single file in ZIP


# Compression detection functions
def _is_gzip(data: bytes) -> bool:
    """Detect GZIP format by magic bytes."""
    return len(data) >= 2 and data[:2] == b'\x1F\x8B'


def _is_zstd(data: bytes) -> bool:
    """Detect Zstandard format by magic bytes."""
    # Zstandard magic number is 0x28 B5 2F FD
    return len(data) >= 4 and data[:4] == b'\x28\xB5\x2F\xFD'


def _is_bz2(data: bytes) -> bool:
    """Detect BZ2 format by magic bytes."""
    return len(data) >= 3 and data[:3] == b'BZh'


def _is_xz(data: bytes) -> bool:
    """Detect XZ format by magic bytes."""
    return len(data) >= 6 and data[:6] == b'\xFD7zXZ\x00'


def _is_zip(data: bytes) -> bool:
    """Detect ZIP format by magic bytes."""
    return len(data) >= 4 and data[:4] == b'PK\x03\x04'


def _is_tar(data: bytes) -> bool:
    """Detect TAR format."""
    import tarfile
    import io
    return tarfile.is_tarfile(io.BytesIO(data))


def _extract_tar(data: bytes) -> bytes:
    """Extract first file from TAR archive."""
    import tarfile
    import io
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:*') as tf:
        first_file = next(f for f in tf if f.isfile())
        return tf.extractfile(first_file).read()


class Compression(StrEnum):
    """Generic compression enum (includes both HTTP and storage compression)"""
    # Dual-purpose (both HTTP and storage)
    GZIP = 'gzip'
    ZSTD = 'zstd'
    # HTTP-only compression
    BZ2 = 'bz2'
    XZ = 'xz'
    ZIP = 'zip'
    # Storage-only compression
    SNAPPY = 'snappy'
    LZ4 = 'lz4'
    BROTLI = 'brotli'

    # NOTE: currently not used, create another @staticmethod compress() similar to decompress() if needed
    def _compress(self, data: bytes, **kwargs) -> bytes:
        """Compress data using this compression method."""
        compression_methods = {
            Compression.GZIP: _compress_gzip,
            Compression.ZSTD: _compress_zstd,
            Compression.BZ2: _compress_bz2,
            Compression.XZ: _compress_xz,
            Compression.ZIP: _compress_zip,
        }
        if self not in compression_methods:
            raise NotImplementedError(
                f"{self} compression is handled by PyArrow, "
                "not available for direct byte compression"
            )
        return compression_methods[self](data, **kwargs)

    def _decompress(self, data: bytes) -> bytes:
        """Decompress data using this compression method."""
        decompression_methods = {
            Compression.GZIP: _decompress_gzip,
            Compression.ZSTD: _decompress_zstd,
            Compression.BZ2: _decompress_bz2,
            Compression.XZ: _decompress_xz,
            Compression.ZIP: _decompress_zip,
        }
        if self not in decompression_methods:
            raise NotImplementedError(
                f"{self} decompression is handled by PyArrow, "
                "not available for direct byte decompression"
            )
        return decompression_methods[self](data)
    
    @staticmethod
    def decompress(data: bytes) -> bytes:
        """Recursively decompress data, handling nested compression and archives."""
        # Detect compression format
        compression = Compression.detect(data)

        if compression:
            # Decompress using detected format
            decompressed = compression._decompress(data)
            return Compression.decompress(decompressed)
        elif _is_tar(data):
            # TAR is an archive format, not compression
            decompressed = _extract_tar(data)
            return Compression.decompress(decompressed)
        else:
            # No compression detected, return as-is
            raise ValueError("Unknown compression type")

    @staticmethod
    def detect(data: bytes) -> 'Compression | None':
        """Detect compression format from data bytes.

        Returns the detected Compression type or None if format cannot be determined.
        Note: Only detects formats with magic bytes (GZIP, ZSTD, BZ2, XZ, ZIP).
        SNAPPY, LZ4, and BROTLI cannot be reliably detected from magic bytes alone.
        """
        if _is_gzip(data):
            return Compression.GZIP
        elif _is_zstd(data):
            return Compression.ZSTD
        elif _is_bz2(data):
            return Compression.BZ2
        elif _is_xz(data):
            return Compression.XZ
        elif _is_zip(data):
            return Compression.ZIP
        return None
