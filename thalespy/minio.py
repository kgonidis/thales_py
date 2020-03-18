from minio import Minio
import re
from datetime import datetime as dt
import pandas as pd
from io import StringIO
from os import environ

minio_client = Minio(
    environ['MINIO_URL'],
    environ['MINIO_PUBKEY'],
    environ['MINIO_PRVKEY'],
    secure=False
)

base_regex = r'(?P<base>\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}-\d+-.+?-.+?)[-,_,\.].*'
ext_regex = r'(?P<base>\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}-\d+-.+?-.+?)[-,_,\.](?P<ext>.*?)\.csv'
error_bucket = 'error-analysis'
run_bucket = 'ngps-processing'
prefixes = {
    'runs': '/runs/',
    'lidar': '/lidar/',
    'ats': '/atsgt/'
}


def check_base(filename, regex=base_regex):
    pattern = re.compile(regex)
    match = pattern.match(filename)
    return match is not None


def get_base(filename, regex=base_regex):
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if match:
        return match.group('base')


def get_ext(filename, regex=ext_regex):
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if match:
        return match.group('ext')


def parse_error_title(ext):
    pattern = re.compile(r'(?P<p1>.*?)_(?P<p2>.*?)(_?)vs_(?P<p3>.*)')
    match = pattern.match(ext)
    if match:
        return match.group('p1'), match.group('p2'), match.group('p3')


def check_date(filename):
    regex=r'(?P<Y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})_(?P<H>\d{2})_(?P<M>\d{2})_(?P<S>\d{2})-(?P<STS>\d.*?)-(?P<RT>.*?)-(?P<OR>.*?).*'
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if not match:
        return None, None, None, None

    year = int(match.group('Y'))
    month = int(match.group('m'))
    day = int(match.group('d'))
    hour = int(match.group('H'))
    minute = int(match.group('M'))
    second = int(match.group('S'))
    date_time = dt(year, month, day, hour, minute, second)
    date_day = date_time.date()
    start_time = date_time.strftime('%H:%M:%S')
    sts_match = int(match.group('STS'))

    return date_time, date_day, start_time, sts_match
    if date is not None and dt(year, month, day).date() != date.date():
        return False
    if (sts is not None and sts > -1) and sts != sts_match:
        return False
    return True

def _list_files(bucket, prefix='', start_date=None, end_date=None, sts=None):
    if end_date is None:
        end_date = dt.now()

    for o in minio_client.list_objects(bucket, prefix):
        m_raw = o.object_name[len(prefix):]
        time, day, start, sts_match = check_date(m_raw)
        if (time is None) or \
            (start_date is not None and start_date <= time <= end_date) or \
            (sts is not None and sts != sts_match):
            continue
        yield {
            'sts': sts_match,
            'time': time,
            'file': m_raw,
            'key': o.object_name,
            'base': get_base(m_raw)
        }


def list_files(bucket, sort=True, prefix='', start_date=None, end_date=None, sts=None):
    files = _list_files(bucket, prefix, start_date, end_date, sts)
    if sort:
        return sorted(files, key=lambda f: [f['sts'], f['time']])
    else:
        return files


def list_run_files(sort=True, start_date=None, end_date=None, sts=None):
    return list_files(run_bucket, sort, prefixes['runs'], start_date, end_date, sts)


def get_run_file(run_file):
    if isinstance(run_file, dict):
        run_file = run_file['file']
    elif '/runs/' not in run_file:
        run_files = '/runs/' + run_file

    m_obj = minio_client.get_object(run_bucket, run_file)
    return m_obj.data


def _list_error_files(date=None, sts=None):
    prefix = ''
    if date is not None:
        prefix = date.strftime('%Y-%m-%d')


    for o in minio_client.list_objects(error_bucket, prefix):
        time, day, start, sts_match = check_date(o.object_name)
        if (time is None) or \
            (sts is not None and sts != sts_match):
            continue

        yield {'sts': sts_match, 'start_time': start, 'file': get_base(o.object_name) }


def list_error_files(sort=True, date=None, sts=None):
    files = _list_error_files(date, sts)
    if sort:
        previous = None
        for f in sorted(files, key=lambda f: [f['sts'], f['start_time']]):
            if previous is None or not f['file'] == previous['file']:
                yield f
            previous = f
    else:
        return files


def download_source_files(run_file):
    pass


def list_matching_error_files(base):

    for o in minio_client.list_objects(error_bucket, base):
        yield o.object_name


def get_error_file(obj_name, **kwargs):
    m_obj = minio_client.get_object(error_bucket, obj_name)
    return pd.read_csv(StringIO(m_obj.data.decode('utf-8')), **kwargs)


