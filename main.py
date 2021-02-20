import gzip
import re
from os.path import isfile
from typing import Dict, Generator, List


def _parse_file(path: str) -> List[Dict]:
    """
    Каждую строчку в файле проверям используя регулярное выражение.
    Используя группирования мы
    Если получены key, value - строка с объявлением ключа и значения.
    Если получено extra_value - строка с продолжением значения.
    """

    pattern = r'(^[^#]\w*\W?\s?\w*?:)(\s*.*$)|(^\s+)([^#].*$)'
    result_dict = dict()
    result = []
    last_key = ''
    rows = get_rows(path)
    for row in rows:
        match = re.findall(pattern, row, re.IGNORECASE)
        if match:
            key, value, _, extra_value = match[0]
            if all((key, value)):
                _parse_key_value_row(key, value, result_dict)
            elif extra_value:
                _parse_extra_value_row(last_key, extra_value, result_dict)
        if row == '\n':
            result.append(_value_to_str(result_dict))
            result_dict = {}
    return result


def get_rows(path: str) -> Generator:
    if path.endswith('.gz'):
        return gzip_file_rows(path)
    return file_rows(path)


def gzip_file_rows(path: str):
    with gzip.open(path) as f:
        for row in f:
            yield row.decode('utf-8')


def file_rows(path: str):
    with open(path) as f:
        for row in f:
            yield row


def _parse_key_value_row(key: str, value: str,  result_dict: Dict) -> None:
    key: str = key.strip(':')
    value: str = value.strip()
    if result_dict.get(key):
        if value not in result_dict[key]:
            result_dict[key].append(value)
    else:
        result_dict[key] = [value]


def _parse_extra_value_row(last_key: str,
                           extra_value: str,
                           result_dict: Dict) -> None:

    if extra_value not in result_dict[last_key]:
        result_dict[last_key].append(extra_value)


def _value_to_str(result_dict: Dict):
    return {
        key: "\n".join(value)
        for key, value in result_dict.items()
    }


def parse_file(path: str) -> List[Dict]:
    if not isfile(path):
        raise FileNotFoundError('File not found!')

    return _parse_file(path)


def load_data(path: str):
    parsed_data = parse_file(path)
    ...


