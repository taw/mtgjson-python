import json
import os
import pathlib
from typing import IO, Optional, Dict, Any

SET_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'set_outputs'
COMP_OUT_DIR = pathlib.Path(__file__).resolve().parent.parent / 'compiled_outputs'
SET_CONFIG_DIR = pathlib.Path(__file__).resolve().parent / 'set_configs'
MTGJSON_CACHE_DIR = pathlib.Path(__file__).resolve().parent.parent / '.mtgjson4_cache'

CACHE_TIMEOUT_SEC = 172800  # 2 Days in seconds


def open_set_json(path: str, mode: str) -> IO:
    """
    Open the set output file for R/W (hopefully reading only)
    and return the IO
    """
    return (SET_OUT_DIR / f'{path}.json').open(mode, encoding='utf-8')


def open_set_config_json(path: str, mode: str) -> IO:
    """
    Open the set config file for R/W (hopefully reading only)
    and return the IO
    """
    file_path = find_file(f'{path}.json', SET_CONFIG_DIR)
    if file_path:
        return pathlib.Path(file_path).open(mode, encoding='utf-8')
    raise KeyError


def write_to_cache_file(file_path: pathlib.Path, text: str) -> None:
    """
    Write content to a cache file (create dir if it doesn't exist)
    """
    os.makedirs(os.path.dirname(str(file_path)), exist_ok=True)
    with pathlib.Path(file_path).open('w', encoding='utf-8') as f:
        f.write(text)


def read_from_cache_file(file_path: pathlib.Path) -> str:
    """
    Read the cache file content
    """
    if file_path.exists():
        with pathlib.Path(file_path).open('r', encoding='utf-8') as f:
            return str(f.read())
    return ''


def find_file(name: str, path: pathlib.Path) -> Optional[str]:
    """
    Function finds where on the path tree a specific file
    can be found. Useful for set_configs as we use sub
    directories to better organize data.
    """
    for root, _, files in os.walk(str(path)):
        if name in files:
            return os.path.join(root, name)
    return None


def is_set_file(path: str) -> bool:
    """
    Function returns if the specific output file
    already exists (useful for determining if a
    foreign lang can be built or not)
    :param path:
    :return:
    """
    joined = SET_OUT_DIR / '{}.json'.format(path)
    return os.path.isfile(joined)


def ensure_set_dir_exists() -> None:
    """
    Function ensures the output directory for sets
    exists, by creating it if necessary
    """
    SET_OUT_DIR.mkdir(exist_ok=True)  # make sure set_outputs dir exists


def remove_null_fields(card_dict: Dict[str, Any]) -> Any:
    """
    Recursively remove all null values found
    """
    if not isinstance(card_dict, (dict, list)):
        return card_dict

    if isinstance(card_dict, list):
        return [v for v in (remove_null_fields(v) for v in card_dict) if v]

    return {k: v for k, v in ((k, remove_null_fields(v)) for k, v in card_dict.items()) if v}


def write_to_compiled_file(file_name: str, file_contents: Dict[str, Any]) -> None:
    """
    Write the compiled data to the specified file
    and return the status of the output.
    Will ensure the output directory exists first
    """
    COMP_OUT_DIR.mkdir(exist_ok=True)
    with pathlib.Path(COMP_OUT_DIR, file_name).open('w', encoding='utf-8') as f:
        new_contents: Dict[str, Any] = remove_null_fields(file_contents)
        json.dump(new_contents, f, indent=4, sort_keys=True, ensure_ascii=False)
