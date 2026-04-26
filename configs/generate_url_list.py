"""
Build the URL list that GCS Storage Transfer Service consumes.

The MediaWiki history dumps are published as monthly snapshots. Each snapshot
contains the full edit history of every wiki, split into per-event-month
TSV.bz2 files. The naming pattern is:

    https://dumps.wikimedia.org/other/mediawiki_history/{SNAPSHOT}/{wiki}/
        {wiki}.{SNAPSHOT}.{event_month}.tsv.bz2

Pick one snapshot (the most recent finished one is best) and pull every
event-month file you want from it.

Usage:
    python generate_url_list.py \
        --snapshot 2026-03 \
        --start 2021-01 --end 2025-12 \
        --wiki enwiki > dump_urls.txt

Open https://dumps.wikimedia.org/other/mediawiki_history/ in a browser to find
the latest finished snapshot folder.
"""

import argparse


def months_between(start_yyyymm: str, end_yyyymm: str):
    sy, sm = [int(x) for x in start_yyyymm.split("-")]
    ey, em = [int(x) for x in end_yyyymm.split("-")]
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


BASE = "https://dumps.wikimedia.org/other/mediawiki_history"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--snapshot", default="2026-03",
                   help="Snapshot folder name on the dump server, e.g. 2025-01")
    p.add_argument("--start", default="2021-01",
                   help="First event month to pull, YYYY-MM")
    p.add_argument("--end", default="2025-12",
                   help="Last event month to pull, YYYY-MM")
    p.add_argument("--wiki", default="enwiki")
    args = p.parse_args()

    # Storage Transfer URL list format wants this header line.
    print("TsvHttpData-1.0")
    for y, m in months_between(args.start, args.end):
        ym = f"{y:04d}-{m:02d}"
        url = (f"{BASE}/{args.snapshot}/{args.wiki}/"
               f"{args.wiki}.{args.snapshot}.{ym}.tsv.bz2")
        # Size and md5 are optional. Leaving size as -1 makes Storage Transfer
        # call HEAD on each URL itself.
        print(f"{url}\t-1\t")


if __name__ == "__main__":
    main()
