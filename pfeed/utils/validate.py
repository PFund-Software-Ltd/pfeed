import sys
import re
import importlib

import click
from rich.console import Console


def validate_pdt(source: str, pdt: str) -> bool:
    SUPPORTED_PRODUCT_TYPES = getattr(importlib.import_module(f'pfeed.sources.{source.lower()}.const'), 'SUPPORTED_PRODUCT_TYPES')
    # REVIEW: pattern: XXX_YYY_PTYPE, hard-coded the max length of XXX and YYY is 8
    pdt_pattern = re.compile(r'^[A-Za-z]{1,8}_[A-Za-z]{1,8}_(.+)$')
    match = pdt_pattern.match(pdt)
    if not match or match.group(1) not in SUPPORTED_PRODUCT_TYPES:
        return False
    else:
        return True


def validate_pdts_and_ptypes(source: str, pdts: list[str], ptypes: list[str], is_cli=False):
    SUPPORTED_PRODUCT_TYPES = getattr(importlib.import_module(f'pfeed.sources.{source.lower()}.const'), 'SUPPORTED_PRODUCT_TYPES')
    if not pdts and not ptypes:
        Console().print(f'Warning: no "--pdts" or "--ptypes" provided, will download ALL products with ALL product types {SUPPORTED_PRODUCT_TYPES} from {source}', style='bold red')
        if is_cli and not click.confirm('Do you want to continue?', default=False):
            sys.exit(1)
    elif pdts and ptypes:
        Console().print(f'Warning: both "--pdts" and "--ptypes" provided, will only use "--pdts" {pdts}', style='bold red')

    for pdt in pdts:
        if not validate_pdt(source, pdt):
            error_msg = f'"{pdt}" does not match the required format "XXX_YYY_PTYPE" or has an unsupported product type. (PTYPE means product type, e.g. PERP, Supported types for {source} are: {SUPPORTED_PRODUCT_TYPES})'
            raise click.BadParameter(error_msg) if is_cli else ValueError(error_msg)

    for ptype in ptypes:
        if ptype not in SUPPORTED_PRODUCT_TYPES:
            error_msg = f'{ptype} is not supported. Supported types are: {SUPPORTED_PRODUCT_TYPES}'
            raise click.BadParameter(error_msg) if is_cli else ValueError(error_msg)
    
    
