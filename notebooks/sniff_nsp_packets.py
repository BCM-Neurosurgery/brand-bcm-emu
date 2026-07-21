#!/usr/bin/env python
"""
Zero-dependency diagnostic: listen to a Cerebus NSP's raw UDP broadcast and
report which Sample Groups (packet types) are actually being broadcast,
and how many channels/samples are in each -- without cbsdk, Central, Redis,
or any other part of BRAND running. Mirrors the packet parsing done by
brand-nsp/nodes/cerebusAdapter/cerebusAdapter.c (legacy 8-byte header).

Usage:
    sudo python sniff_nsp_packets.py <broadcast_device> [--port 51002]
                                      [--ip 192.168.137.255] [--duration 5]

Example (matches dual_nsp_audio.yaml's NSP1):
    sudo python sniff_nsp_packets.py enp4s0

Notes:
- Needs root (or CAP_NET_RAW) for SO_BINDTODEVICE, same as cerebusAdapter
  itself when run outside of `booter`'s usual sudo wrapper.
- Sample Group IDs (the packet `type` field) map to rates as documented by
  Blackrock in cbproto.h: 1=500Hz, 2=1kHz, 3=2kHz, 4=10kHz, 5=30kHz,
  6=Raw(30kHz, unfiltered). `type == 0` with `dlen > 0` is a spike packet,
  not a continuous Sample Group.
- `dlen` is the packet's payload length in 4-byte words; channel count for
  that packet = dlen * 2 (each channel's sample is one int16 = 2 bytes).
  This can tell you which groups exist and how many channels are in them,
  but NOT which physical electrode/BNC channel is at which position within
  the payload -- the wire format's `chid` field is always 0 for Sample
  Group packets (see the cbPKT_GROUP comment in cbproto.h), so per-channel
  identity is not self-describing on the wire. To resolve that, you need
  either Central's channel/group config directly, or an empirical test
  (e.g. wiggle one BNC input while watching which buffer column changes).
"""
import argparse
import socket
import struct
import time
from collections import defaultdict

HEADER_FMT = "<IHBB2x"  # legacy cerebus_packet_header_t: uint32 time, uint16 chid, uint8 type, uint8 dlen, 2 reserved
HEADER_SIZE = struct.calcsize(HEADER_FMT)

RATE_BY_GROUP = {1: "500Hz", 2: "1kHz", 3: "2kHz", 4: "10kHz", 5: "30kHz", 6: "Raw(30kHz)"}


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("broadcast_device", help="NIC bound to this NSP, e.g. enp4s0")
    ap.add_argument("--ip", default="192.168.137.255", help="subnet broadcast address")
    ap.add_argument("--port", type=int, default=51002, help="Cerebus data broadcast port")
    ap.add_argument("--duration", type=float, default=5.0, help="seconds to listen")
    return ap.parse_args()


def open_socket(broadcast_ip, port, device):
    fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    fd.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    fd.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO,
                  struct.pack("ll", 1, 0))  # 1s timeout, so Ctrl-C works
    try:
        fd.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, device.encode())
    except PermissionError:
        raise SystemExit(
            f"SO_BINDTODEVICE on {device!r} needs root -- rerun with sudo "
            f"(same requirement cerebusAdapter itself has).")
    fd.bind((broadcast_ip, port))
    return fd


def main():
    args = parse_args()
    fd = open_socket(args.ip, args.port, args.broadcast_device)
    print(f"Listening on {args.broadcast_device} ({args.ip}:{args.port}) "
          f"for {args.duration:.0f}s...")

    # per packet-type: count seen, and the set of distinct dlen values seen
    groups = defaultdict(lambda: {"count": 0, "dlens": set()})
    spike_pkts = 0
    n_udp_datagrams = 0

    t_end = time.monotonic() + args.duration
    while time.monotonic() < t_end:
        try:
            payload, _ = fd.recvfrom(65535)
        except socket.timeout:
            continue
        n_udp_datagrams += 1

        # a UDP datagram is a sequence of concatenated Cerebus packets
        i = 0
        while i + HEADER_SIZE <= len(payload):
            _time, chid, ptype, dlen = struct.unpack_from(HEADER_FMT, payload, i)
            if ptype == 0 and dlen > 0:
                spike_pkts += 1
            else:
                groups[ptype]["count"] += 1
                groups[ptype]["dlens"].add(dlen)
            i += HEADER_SIZE + dlen * 4

    fd.close()

    print(f"\n{n_udp_datagrams} UDP datagrams received.")
    if spike_pkts:
        print(f"{spike_pkts} spike/heartbeat packets (type=0) seen.")
    if not groups:
        print("No Sample Group packets seen -- check broadcast_device/ip/port, "
              "and that the NSP is actually running/broadcasting.")
        return

    print("\nSample Groups observed:")
    print(f"{'type (packet_type)':>20} {'rate':>10} {'count':>10} {'dlen (words)':>14} {'-> channels (dlen*2)':>22}")
    for ptype in sorted(groups):
        info = groups[ptype]
        rate = RATE_BY_GROUP.get(ptype, "?")
        dlens = sorted(info["dlens"])
        chans = sorted(d * 2 for d in dlens)
        print(f"{ptype:>20} {rate:>10} {info['count']:>10} {str(dlens):>14} {str(chans):>22}")

    print("\nUse these `type` values as `packet_type` and the channel counts "
          "(dlen*2) as a lower bound for `chan_per_stream` in the graph YAML "
          "(round up, never down -- see brand-nsp's cerebusAdapter memory-safety "
          "gotcha in CLAUDE.md). This does NOT tell you which channel index is "
          "neural vs. behavioral within a group -- see the module docstring.")


if __name__ == "__main__":
    main()
