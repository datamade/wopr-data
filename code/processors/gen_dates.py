from datetime import datetime, timedelta, date
import calendar

def add_month(sourcedate):
    month = sourcedate.month
    year = sourcedate.year + month / 12
    month = month %12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return date(year,month,day)

def datespan(start):
    end = datetime.now()
    current = start
    delta = timedelta(days=30)
    while (current.year, current.month) != (end.year, end.month):
        yield current
        current = add_month(current)

if __name__ == "__main__":
    import sys
    ext = sys.argv[1]
    try:
        start = datetime.strptime(sys.argv[2], '%Y/%m')
    except IndexError:
        start = datetime(1996,7,1)
    outp = []
    for d in datespan(start):
        outp.append('%s%s' % (d.strftime('%Y%m'),ext))
    sys.stdout.write(' '.join(outp))
