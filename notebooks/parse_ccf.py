#!/usr/bin/env python
"""
Parse a Blackrock Central .ccf config file and summarize, per NSP, which
Sample Group (BRAND's `packet_type`) each channel category (neural
front-end, BNC analog-input, digital-input, etc.) is assigned to.

.ccf files are plain XML (Central writes them as a serialized cbPKT_CHANINFO
per channel), so this needs zero cbsdk/Central/network dependency -- it just
reads the file straight off disk.

Usage:
    python parse_ccf.py "/path/to/some.ccf"

Channel category is inferred from Blackrock's fixed bank layout
(cbproto.h): each processor (NSP) gets 14 banks in order --
banks 1-8 = Front-End (32 channels each = 256 total), 9 = Analog-Input
(16-channel BNC bank), 10 = Analog-Output, 11 = Audio-Output,
12 = Digital-Input, 13 = Serial-Input, 14 = Digital-Output. Banks are
numbered globally across processors (proc 2's Front-End banks start at 15,
not 1), which is why this offsets by `(proc - 1) * 14` before classifying.

Sample Group IDs (rate mapping) are Blackrock's fixed convention:
0=disabled, 1=500Hz, 2=1kHz, 3=2kHz, 4=10kHz, 5=30kHz, 6=Raw(30kHz).
`sample/group` is only meaningful for continuously-sampled channel types
(Front-End, Analog-Input) -- Digital-Input/Serial-Input are event-driven
(cbPKT_DINP-style) and always show group 0 here regardless of whether
they're actually in use.

Caveat: a .ccf only tells you what *that saved config* specifies -- it may
not match what's currently loaded on a live NSP. Cross-check against
Central directly, or empirically against `sniff_nsp_packets.py` /
`pycbsdk`, if there's any doubt the file is current.
"""
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict

FE_BANKS = 8          # 256 front-end chans / 32 chans per bank
BANKS_PER_PROC = 14   # FE(8) + ANAIN(1) + ANAOUT(1) + AUDOUT(1) + DIGIN(1) + SERIAL(1) + DIGOUT(1)

CATEGORY_BY_LOCAL_BANK = {
    FE_BANKS + 1: "Analog-Input (BNC)",
    FE_BANKS + 2: "Analog-Output",
    FE_BANKS + 3: "Audio-Output",
    FE_BANKS + 4: "Digital-Input",
    FE_BANKS + 5: "Serial-Input",
    FE_BANKS + 6: "Digital-Output",
}

RATE_BY_GROUP = {0: "disabled", 1: "500Hz", 2: "1kHz", 3: "2kHz", 4: "10kHz", 5: "30kHz", 6: "Raw(30kHz)"}


def bank_category(bank, proc):
    local = bank - (proc - 1) * BANKS_PER_PROC
    if 1 <= local <= FE_BANKS:
        return "Front-End (neural)"
    return CATEGORY_BY_LOCAL_BANK.get(local, f"local-bank-{local}(?)")


def parse(path):
    rows = []
    for _, elem in ET.iterparse(path, events=("end",)):
        if elem.tag == "ChanInfo_item":
            label = elem.get("label", "")

            def get_int(p):
                e = elem.find(p)
                return int(e.text) if e is not None and e.text is not None else None

            rows.append((get_int("proc"), get_int("bank"), get_int("chan"),
                         get_int("sample/group"), label))
            elem.clear()  # keep memory bounded -- these files run ~15MB/500+ channels
    return rows


def summarize(rows):
    summary = defaultdict(list)
    labels = {}
    for proc, bank, chan, smp_group, label in rows:
        key = (proc, bank_category(bank, proc), smp_group)
        summary[key].append(chan)
        labels.setdefault(key, label)
    return summary, labels


def main():
    if len(sys.argv) < 2:
        raise SystemExit(f"Usage: python {sys.argv[0]} <path-to.ccf>")
    path = sys.argv[1]
    rows = parse(path)
    print(f"Parsed {len(rows)} ChanInfo_item entries from {path}\n")

    summary, labels = summarize(rows)
    print(f"{'NSP(proc)':>10} {'category':>20} {'group':>6} {'rate':>10} {'n_chans':>8} {'chan range':>12}  example label")
    for (proc, cat, smp_group), chans in sorted(summary.items(), key=lambda x: (x[0][0], x[0][1], x[0][2] or -1)):
        rate = RATE_BY_GROUP.get(smp_group, "?")
        cs = sorted(chans)
        print(f"{proc!s:>10} {cat:>20} {smp_group!s:>6} {rate:>10} {len(chans):>8} "
              f"{f'{cs[0]}-{cs[-1]}':>12}  {labels[(proc, cat, smp_group)]}")

    print("\nRows with group==0 for Front-End/Analog-Input are channels present in this\n"
          "bank but NOT assigned to any continuous Sample Group (i.e. not streaming).\n"
          "Digital-Input/Serial-Input always show group 0 here regardless of use --\n"
          "they're event-driven (cbPKT_DINP-style), not part of any Sample Group.")


if __name__ == "__main__":
    main()
