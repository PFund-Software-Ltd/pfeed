import io
import csv
import gzip
import zipfile
import tarfile

import pandas as pd


def is_parquet(data):
    return data[:4] == b'PAR1'


def is_gzip(data):
    return data[:2] == b'\x1F\x8B'


def is_zip(data):
    return data[:4] == b'PK\x03\x04'


def is_tar(data):
    return tarfile.is_tarfile(io.BytesIO(data))


def is_likely_csv(data, sample_size=4096):
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


def decompress_gzip(data):
    return gzip.decompress(data)


def decompress_zip(data):
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        return zf.read(zf.namelist()[0])  # Assumes single file in ZIP


def extract_tar(data):
    with tarfile.open(fileobj=io.BytesIO(data), mode='r:*') as tf:
        first_file = next(f for f in tf if f.isfile())
        return tf.extractfile(first_file).read()
    

def read_raw_data(data: bytes) -> pd.DataFrame:
    if is_gzip(data):
        decompressed = decompress_gzip(data)
        return read_raw_data(decompressed)  # Recursive call with decompressed data
    elif is_zip(data):
        decompressed = decompress_zip(data)
        return read_raw_data(decompressed)
    elif is_tar(data):
        extracted = extract_tar(data)
        return read_raw_data(extracted)
    elif is_parquet(data):
        return pd.read_parquet(io.BytesIO(data))
    elif is_likely_csv(data):
        return pd.read_csv(io.BytesIO(data))
    else:
        raise ValueError("Unknown or unsupported format")
