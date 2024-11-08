import io

import pandas as pd


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


def decompress_gzip(data: bytes) -> bytes:
    import gzip
    return gzip.decompress(data)


def decompress_bz2(data: bytes) -> bytes:
    import bz2
    return bz2.decompress(data)


def decompress_xz(data: bytes) -> bytes:
    import lzma
    return lzma.decompress(data)


def decompress_zstd(data: bytes) -> bytes:
    import zstandard as zstd
    dctx = zstd.ZstdDecompressor()
    return dctx.decompress(data)
    
    
def decompress_zip(data: bytes) -> bytes:
    import zipfile
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return zf.read(zf.namelist()[0])  # Assumes single file in ZIP


def extract_tar(data: bytes) -> bytes:
    import tarfile
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:*') as tf:
        first_file = next(f for f in tf if f.isfile())
        return tf.extractfile(first_file).read()
    

def convert_raw_data_to_pandas_df(data: bytes) -> pd.DataFrame:
    if is_gzip(data):
        decompressed = decompress_gzip(data)
        return convert_raw_data_to_pandas_df(decompressed)  # Recursive call with decompressed data
    elif is_zstd(data):
        decompressed = decompress_zstd(data)
        return convert_raw_data_to_pandas_df(decompressed)
    elif is_bz2(data):
        decompressed = decompress_bz2(data)
        return convert_raw_data_to_pandas_df(decompressed)
    elif is_xz(data):
        decompressed = decompress_xz(data)
        return convert_raw_data_to_pandas_df(decompressed)
    elif is_zip(data):
        decompressed = decompress_zip(data)
        return convert_raw_data_to_pandas_df(decompressed)
    elif is_tar(data):
        extracted = extract_tar(data)
        return convert_raw_data_to_pandas_df(extracted)
    elif is_parquet(data):
        return pd.read_parquet(io.BytesIO(data))
    elif is_likely_csv(data):
        return pd.read_csv(io.BytesIO(data))
    else:
        raise ValueError("Unknown or unsupported format")
