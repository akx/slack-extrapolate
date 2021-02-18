import pandas as pd
import datetime
import dateutil.rrule as rr
from scipy.optimize import curve_fit

MAGIC_MEMBER_COUNT = 10000


def first_degree_poly(x, a, b):
    return a * x + b


def second_degree_poly(x, a, b, c):
    return a * x + b * x ** 2 + c


def third_degree_poly(x, a, b, c, d):
    return a * x + b * x ** 2 + c * x ** 3 + d


def fourth_degree_poly(x, a, b, c, d, e):
    return a * x + b * x ** 2 + c * x ** 3 + d * x ** 4 + e


def read_data():
    df = pd.read_csv(
        "joinsperweek.tsv",
        delimiter="\t",
        header=None,
        names=["yw", "joins"],
        parse_dates=True,
    )
    df["monday"] = df["yw"].apply(
        lambda s: datetime.datetime.strptime(s + "D0", "%YW%WD%w")
    )
    df["cum"] = df.joins.cumsum()
    dates = pd.to_numeric(df.monday).to_numpy() / 1_000_000_000
    user_count = df.cum.to_numpy()
    return dates, user_count


def curve_fit_data(curve_func, dates, user_count):
    popt, _ = curve_fit(curve_func, dates, user_count)

    dt: datetime.datetime
    for dt in rr.rrule(
        freq=rr.WEEKLY,
        byweekday=rr.MO,
        dtstart=datetime.datetime.fromtimestamp(max(dates)),
    ):
        ts = dt.timestamp()
        n = curve_func(ts, *popt)
        if n >= MAGIC_MEMBER_COUNT:
            print(curve_func.__name__, dt, int(n), sep="\t")
            break


def main():
    dates, user_count = read_data()
    curve_fit_data(first_degree_poly, dates, user_count)
    curve_fit_data(second_degree_poly, dates, user_count)
    curve_fit_data(third_degree_poly, dates, user_count)
    curve_fit_data(fourth_degree_poly, dates, user_count)


if __name__ == "__main__":
    main()
