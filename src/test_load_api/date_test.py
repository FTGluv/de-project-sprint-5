import datetime

test_date = datetime.datetime.today()

from_date = datetime.date(test_date.year, test_date.month, test_date.day)
to_date = datetime.date(test_date.year, test_date.month, test_date.day + 1)

print(from_date.strftime('%Y-%m-%d %H:%M:%S'))
print(to_date.strftime('%Y-%m-%d %H:%M:%S'))

