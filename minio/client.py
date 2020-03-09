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


error_bucket = 'error-analysis'


def check_date(filename):
    regex=r'(?P<Y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})_(?P<H>\d{2})_(?P<M>\d{2})_(?P<S>\d{2})-(?P<STS>\d.*?)-(?P<RT>.*?)-(?P<OR>.*?).*\.csv'
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


def list_error_files(date=None, sts=None):
    prefix = ''
    if date is not None:
        prefix = date.strftime('%Y-%m-%d')


    for o in minio_client.list_objects(error_bucket, prefix):
        time, day, start, sts_match = check_date(o.object_name)
        if (time is None) or \
            (sts is not None and sts != sts_match):
            continue

        yield {'sts': sts_match, 'start_time': start, 'file': o.object_name }


def get_error_file(obj_name, **kwargs):
    m_obj = minio_client.get_object(error_bucket, obj_name)
    return pd.read_csv(StringIO(m_obj.data.decode('utf-8')), **kwargs)

