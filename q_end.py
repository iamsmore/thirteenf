import datetime
import calendar



#get prior quarter
def get_prior_quarter(q):
    prior = q - 1
    if prior == 0:
        prior = 4
    return prior

#get current quarter
def get_quarter(date):
    return (date.month - 1) / 3 + 1

#get prior weekday for given date
def prev_weekday(date):
    date -= datetime.timedelta(days=1)
    while date.weekday() > 4:
        date -= datetime.timedelta(days=1)
    return date

#get prior quarter end for a given date
def get_qend(date):
    quarter = get_prior_quarter(int(get_quarter(date)))
    month = quarter * 3
    year = date.year
    day = calendar.monthrange(year, month)[1]
    quarterEnd = datetime.date(year, month, day)
    if quarterEnd.weekday() > 4:
        return prev_weekday(quarterEnd)
    else:
        return quarterEnd
