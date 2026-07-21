#!/usr/bin/env python
"""
Live equivalent of parse_ccf.py: connect to both NSPs via pycbsdk (run in
the `nsp_env` conda env) and print, per NSP, which Sample Group
(BRAND's `packet_type`) each channel category is currently assigned to.

Unlike parse_ccf.py (which reads a saved-to-disk .ccf snapshot), this
queries the NSP directly, so it reflects whatever is actually loaded right
now -- use this to confirm a .ccf-derived graph config (e.g.
graphs/dual_nsp_audio.yaml, currently based on YFY_spikeAutoThresh.ccf)
still matches reality before trusting it for a session.

Usage (from nsp_env):
    conda activate nsp_env
    python poll_nsp_config.py

Both NSPs sit behind the same device_address on this rig -- they're on
isolated point-to-point links, not a shared broadcast subnet with distinct
per-NSP addresses -- so they're told apart by which local NIC the client
socket is bound to (`client_interface`), not by device_address:
    NSP1: Session(DeviceType.LEGACY_NSP, device_address="192.168.137.128")
    NSP2: Session(DeviceType.LEGACY_NSP, device_address="192.168.137.128",
                   client_interface="enp5s0f3")
This mirrors cerebusAdapterGem.c's SO_BINDTODEVICE mechanism -- NOT what
the legacy (non-Gem) `cerebusAdapter.c` node does, which ignores
broadcast_device/broadcast_ip entirely (see the CLAUDE.md gotcha on this).
"""
import sys

from pycbsdk import ChannelType, ChanInfoField, DeviceType, Session

NSP_CONNECTIONS = {
    "NSP1": dict(device_type=DeviceType.LEGACY_NSP, device_address="192.168.137.128"),
    "NSP2": dict(device_type=DeviceType.LEGACY_NSP, device_address="192.168.137.128",
                 client_interface="enp5s0f3"),
}

RATE_BY_GROUP = {0: "disabled", 1: "500Hz", 2: "1kHz", 3: "2kHz", 4: "10kHz", 5: "30kHz", 6: "Raw(30kHz)"}


def poll_nsp(nsp_name, session_kwargs):
    rows = []
    with Session(**session_kwargs) as session:
        for ct in ChannelType:
            ids = session.get_matching_channel_ids(ct)
            if not ids:
                continue
            groups = session.get_channels_field(ct, ChanInfoField.SMPGROUP)
            for ch, grp in zip(ids, groups):
                label = session.get_channel_label(ch)
                rows.append((ct.name, ch, grp, label))
    return rows


def summarize(nsp_name, rows):
    from collections import defaultdict
    by_key = defaultdict(list)
    labels = {}
    for cat, ch, grp, label in rows:
        key = (cat, grp)
        by_key[key].append(ch)
        labels.setdefault(key, label)

    print(f"\n=== {nsp_name} ===")
    print(f"{'category':>15} {'group':>6} {'rate':>10} {'n_chans':>8} {'chan range':>12}  example label")
    for (cat, grp), chans in sorted(by_key.items(), key=lambda x: (x[0][0], x[0][1] or -1)):
        rate = RATE_BY_GROUP.get(grp, "?")
        cs = sorted(chans)
        print(f"{cat:>15} {grp!s:>6} {rate:>10} {len(cs):>8} {f'{cs[0]}-{cs[-1]}':>12}  {labels[(cat, grp)]}")


def main():
    for nsp_name, kwargs in NSP_CONNECTIONS.items():
        try:
            rows = poll_nsp(nsp_name, kwargs)
        except Exception as exc:
            print(f"\n=== {nsp_name} === FAILED TO CONNECT: {exc}", file=sys.stderr)
            continue
        summarize(nsp_name, rows)

    print("\nCompare this against parse_ccf.py's output for the same .ccf this "
          "graph is based on -- if they disagree, the live config has drifted "
          "from the reference file and graphs/dual_nsp_audio.yaml needs updating.")


if __name__ == "__main__":
    main()
