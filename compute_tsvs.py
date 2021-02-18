import argparse
import datetime
import json
import os
import zipfile
from collections import defaultdict, Counter

import tqdm


def get_message_dates_and_users(zips):
    message_dates = defaultdict(set)
    message_dates_with_sys = defaultdict(set)
    users = {}
    for zipname in zips:
        with zipfile.ZipFile(zipname) as zf:
            for name in tqdm.tqdm(zf.namelist(), desc=zipname):
                if name.endswith(".json") and os.path.basename(name)[0].isdigit():
                    for ent in json.loads(zf.read(name)):
                        if ent.get("type") != "message":
                            continue
                        userid = ent.get("user")
                        if not userid:
                            continue
                        ts = ent.get("ts")
                        if not ts:
                            continue
                        ts = int(float(ts))
                        if not ent.get("subtype"):
                            message_dates[userid].add(ts)
                        message_dates_with_sys[userid].add(ts)
                        prof = ent.get("user_profile")
                        if prof:
                            users[userid] = prof
    return message_dates, message_dates_with_sys, users


def write_first_per_week(tsv_file, message_dates):
    first_message_dates = {uid: min(dates) for (uid, dates) in message_dates.items()}
    first_messages_per_week = Counter()
    for uid, ts in first_message_dates.items():
        year, week, *_ = datetime.datetime.utcfromtimestamp(ts).isocalendar()
        first_messages_per_week[f"{year}W{week:02d}"] += 1
    for yw, n in sorted(first_messages_per_week.items()):
        print(yw, n, sep="\t", file=tsv_file)


def write_active_users_per_week(tsv_file, message_dates):
    min_ts = min(min(dates) for dates in message_dates.values())
    max_ts = max(max(dates) for dates in message_dates.values())
    interval = 7 * 86400
    active_users_per_week = {}
    ts1s = range(min_ts, max_ts, interval)

    def compute_week_info(ts1):
        year, week, *_ = datetime.datetime.utcfromtimestamp(ts1).isocalendar()
        yw = f"{year}W{week:02d}"
        ts2 = ts1 + interval
        aupw = len(
            [
                uid
                for uid, dates in message_dates.items()
                if any(ts1 < d < ts2 for d in dates)
            ]
        )
        return (yw, aupw)

    for ts1 in tqdm.tqdm(ts1s):
        yw, aupw = compute_week_info(ts1)
        active_users_per_week[yw] = aupw
    for yw, n in sorted(active_users_per_week.items()):
        print(yw, n, sep="\t", file=tsv_file)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zips", nargs="*")
    args = ap.parse_args()
    if not args.zips:
        ap.error("zips missing")
    message_dates, message_dates_with_sys, users = get_message_dates_and_users(
        args.zips
    )
    with open("firstmessagesperweek.tsv", "w") as outf:
        write_first_per_week(outf, message_dates)
    with open("joinsperweek.tsv", "w") as outf:
        write_first_per_week(outf, message_dates_with_sys)
    with open("activeperweek.tsv", "w") as outf:
        write_active_users_per_week(outf, message_dates)


if __name__ == "__main__":
    main()
