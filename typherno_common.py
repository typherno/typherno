# Copyright 2011-2013 Nate Diller
# All rights reserved

# not for distribution

import struct
import math
import urllib
import time

CTL		= 0x01
SET		= 0xcb

PROVISION	= 0x60
ENTRY		= 0x18

DATA		= 0x42
DATA_ACK	= 0x7c

RECLAIM		= 0xa5


"""
msg_format = { \
  CTL: "!B", \	# msg_id {ctl_str,}
  SET: "!BB", \		# channel {ctl_str,}

  PROVISION: "!BBQ", \		# offset {path}
  ENTRY: "!BB", \		# {modepath}

  DATA: "!BB", \		# {data}
  DATA_ACK: "!BBL", \		# cookie {data}

  RECLAIM: "!BB", \	# channel
} """

msg_format = { \
  CTL: "!B", \
  SET: "!BB", \

  PROVISION: "!BBQ", \
  ENTRY: "!BB", \

  DATA: "!BB", \
  DATA_ACK: "!BBL", \

  RECLAIM: "!BB", \
}


FS_PATH_MAX = 2**12
FS_PIECE_LENGTH = 2**18

AR_SEGMENT_CAP = 63

AR_WRITE_MAX = 2**22
AR_QUEUE_SENDBUF = 2**12


PSTR = "Typherno protocol"
HANDSHAKE = struct.pack("!B", len(PSTR)) + PSTR + struct.pack("!BBL", 0, 0, 0)


def verify_handshake(msg):
	cmp = 1 + len(PSTR)
	if msg[:cmp] != HANDSHAKE[:cmp]:
		raise RuntimeError("Handshake: protocol mismatch")

	version, revision, flags = struct.unpack("!BBL", msg[cmp:])
	if version != 0:
		raise RuntimeError("Handshake: wrong protocol version")

	return revision, flags


def msg_generator(data, cmd, *args):
	fmt = msg_format[cmd]
	if not isinstance(data, str):
		raise RuntimeError(data)
	yield struct.pack("!L", struct.calcsize(fmt) + len(data))
	yield struct.pack(fmt, cmd, *args)
	yield data


def seq_generator(*seq):
	for s in seq:
		yield struct.pack("!B", len(s))
		yield s


def raw_msg(data, cmd, *args):
	return ''.join(msg_generator(data, cmd, *args))


def ctl_msg(*seq):
	return raw_msg(''.join(seq_generator(*seq)), CTL)


def cmd_msg(ch, *seq):
	return raw_msg(''.join(seq_generator(*seq)), SET, ch)


def mb_str(bytes, precision=2):
	suffixes = ['KB', 'MB','GB','TB','PB','EB', 'ZB', 'YB']
	num = float(bytes) / 2**10
	if num < 1:
		return "%d B" % bytes

	while num > 1024:
		num = num / 2**10
		suffixes.pop(0)
	if precision > math.log10(num):
		return "%.*g%s" % (precision, num, suffixes[0])

	return "%d%s" % (round(num, precision - int(math.floor(math.log10(num))) - 1), suffixes[0])


def time_stamp(*args):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(*args))


def page_opener(host, port, path):
	opener = urllib.FancyURLopener()

	page = opener.open("http://%s:%d/%s" % (host, port, path))
	for line in page:
		line = line.strip()
		if line:
			yield line
	page.close()


