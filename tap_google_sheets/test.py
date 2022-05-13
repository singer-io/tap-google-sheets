import dateutil.parser
import pytz
import datetime

DATETIME_PARSE = "%Y-%m-%dT%H:%M:%SZ"
DATETIME_FMT = "%04Y-%m-%dT%H:%M:%S.%fZ"
DATETIME_FMT_SAFE = "%Y-%m-%dT%H:%M:%S.%fZ"

def strptime_to_utc(dtimestr):
    d_object = dateutil.parser.parse(dtimestr)
    if d_object.tzinfo is None:
        return d_object.replace(tzinfo=pytz.UTC)
    else:
        return d_object.astimezone(tz=pytz.UTC)

def strftime(dtime, format_str=DATETIME_FMT):
    if dtime.utcoffset() != datetime.timedelta(0):
        raise Exception("datetime must be pegged at UTC tzoneinfo")

    dt_str = None
    try:
        dt_str = dtime.strftime(format_str)
        if dt_str.startswith('4Y'):
            dt_str = dtime.strftime(DATETIME_FMT_SAFE)
    except ValueError:
        dt_str = dtime.strftime(DATETIME_FMT_SAFE)

    return dt_str

date1 =  (strptime_to_utc('-001-11-21 00:00:00'))
print(date1)