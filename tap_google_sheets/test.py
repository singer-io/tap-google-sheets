import cftime

units ="seconds since 1970-01-01T00:00:00Z"
time = -694000
# time = 5000000
epoch = (time - 25569)*86400
print(epoch)
date =cftime.num2date(epoch, units, calendar='proleptic_gregorian', only_use_cftime_datetimes=True, only_use_python_datetimes=False, has_year_zero=True)
print(date)
print(type(date))
date = date.strftime()
print(f'{date}   {type(date)}')