import io


def is_parquet(data: bytes) -> bool:
    return data[:4] == b'PAR1'


def is_gzip(data: bytes) -> bool:
    return data[:2] == b'\x1F\x8B'


def is_bz2(data: bytes) -> bool:
    return data[:3] == b'BZh'


def is_xz(data: bytes) -> bool:
    return data[:6] == b'\xFD7zXZ\x00'


def is_zip(data: bytes) -> bool:
    return data[:4] == b'PK\x03\x04'


def is_zstd(data: bytes) -> bool:
    # Zstandard magic number is 0x28 B5 2F FD
    return data[:4] == b'\x28\xB5\x2F\xFD'


def is_tar(data: bytes) -> bool:
    import tarfile
    return tarfile.is_tarfile(io.BytesIO(data))


def is_likely_csv(data: bytes, sample_size: int = 4096) -> bool:
    import csv
    try:
        # Try to decode a sample of the data
        sample = data[:sample_size].decode('utf-8', errors='ignore')
        lines = sample.splitlines()
        if len(lines) < 2:
            return False
        
        # Use csv.Sniffer to detect if it's a CSV and what the dialect is
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        has_header = sniffer.has_header(sample)
        
        # If we've gotten this far without an exception, it's likely a CSV
        return True
    except:
        # If any exception occurred, it's probably not a CSV
        return False


def compress_gzip(data: bytes, compression_level: int = 9) -> bytes:
    import gzip
    return gzip.compress(data, compresslevel=compression_level)


def decompress_gzip(data: bytes) -> bytes:
    import gzip
    return gzip.decompress(data)


def compress_bz2(data: bytes, compression_level: int = 9) -> bytes:
    import bz2
    return bz2.compress(data, compresslevel=compression_level)


def decompress_bz2(data: bytes) -> bytes:
    import bz2
    return bz2.decompress(data)


def compress_xz(data: bytes, compression_level: int = 6) -> bytes:
    import lzma
    return lzma.compress(data, preset=compression_level)


def decompress_xz(data: bytes) -> bytes:
    import lzma
    return lzma.decompress(data)


def compress_zstd(data: bytes) -> bytes:
    import zstandard as zstd
    cctx = zstd.ZstdCompressor()
    return cctx.compress(data)


def decompress_zstd(data: bytes) -> bytes:
    import zstandard as zstd
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(data)

    
def compress_zip(data: bytes, filename: str = "file.txt", compression_level: int = 6) -> bytes:
    import zipfile
    import io
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=compression_level) as zf:
        zf.writestr(filename, data)
    return buffer.getvalue()

    
def decompress_zip(data: bytes) -> bytes:
    import zipfile
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return zf.read(zf.namelist()[0])  # Assumes single file in ZIP


def extract_tar(data: bytes) -> bytes:
    import tarfile
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:*') as tf:
        first_file = next(f for f in tf if f.isfile())
        return tf.extractfile(first_file).read()
    

def decompress_data(data: bytes) -> bytes:
    if is_gzip(data):
        decompressed = decompress_gzip(data)
        return decompress_data(decompressed)
    elif is_zstd(data):
        decompressed = decompress_zstd(data)
        return decompress_data(decompressed)
    elif is_bz2(data):
        decompressed = decompress_bz2(data)
        return decompress_data(decompressed)
    elif is_xz(data):
        decompressed = decompress_xz(data)
        return decompress_data(decompressed)
    elif is_zip(data):
        decompressed = decompress_zip(data)
        return decompress_data(decompressed)
    elif is_tar(data):
        decompressed = extract_tar(data)
        return decompress_data(decompressed)
    return data


compression_methods = {
    'zstd': compress_zstd,
    'gzip': compress_gzip,
    'bz2': compress_bz2,
    'xz': compress_xz,
    'zip': compress_zip,
}


decompression_methods = {
    'zstd': decompress_zstd,
    'gzip': decompress_gzip,
    'bz2': decompress_bz2,
    'xz': decompress_xz,
    'zip': decompress_zip,
}