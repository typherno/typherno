#
# Copyright (c) 2012, 2013 Nate Diller.
# All rights reserved.
# This component and the accompanying materials are made available
# under the terms of the "Eclipse Public License v1.0"
# which accompanies this distribution, and is available
# at the URL "http://www.eclipse.org/legal/epl-v10.html".
#
# Initial Contributors:
# Nate Diller - Initial contribution.
#
# Contributors:
# 
# Description:
#
#

import socket
import struct
import sys

from typherno_common import *


def unpack_seq(ctl_str):
	seq = []

	while ctl_str:
		c, ctl_str = ctl_str[:1], ctl_str[1:]

		bytes, = struct.unpack("!B", c)
		if bytes > len(ctl_str):
			raise RuntimeError("Too few bytes in ctl seq, expected %d, only %d remaining" % (bytes, len(ctl_str)))

		this, ctl_str = ctl_str[:bytes], ctl_str[bytes:]
		seq.append(this)

	return seq


class msg_socket(socket.socket):
	def __init__(self):
		socket.socket.__init__(self, socket.AF_INET, socket.SOCK_STREAM)

		self.peer_uuid = ''
		self.peer_role = ''

		self.peer_info = {}

		self.log_file = None

	def _print(self, out):
		if self.log_file:
			self.log_file.write("%s\n" % out)
			self.log_file.flush()
		else:
			sys.stdout.write("%s\n" % out)
			sys.stdout.flush()

	def parse_info(self, msg_id, ctl_str):
		if not (msg_id == CTL):
			raise RuntimeError("Invalid server response, msg_id=%d" % msg_id)

		seq = unpack_seq(ctl_str)

		self.peer_uuid = seq.pop(0)
		self.peer_role = seq.pop(0)

		if seq.pop(0) != "Info":
			raise RuntimeError("info: invalid sequence")

		while seq:
			self.peer_info[seq.pop(0)] = seq.pop(0)

		self._print("--- %s Info ---" % self.peer_role)
		self._print("%s:\t%s" % (self.peer_role, self.peer_uuid))
		for k, v in self.peer_info.items():
			self._print("%s:\t%s" % (k, v))
		self._print('')

	def connected(self):
		pass

	def connect(self, *sockargs):
		socket.socket.connect(self, *sockargs)

		self.connected()

		self._print("Connected, sending handshake")
		self.send(HANDSHAKE)
		verify_handshake(self.recv_bytes(len(HANDSHAKE)))
		self._print("Remote handshake verified, waiting for info structure...")

		self.parse_info(*self.get_msg())

		self.handle_connect()

	def recv_bytes(self, bytes, msg=''):
		while len(msg) < bytes:
			piece = self.recv(bytes - len(msg))
			if not piece:
				raise RuntimeError("Connection dropped")
			msg += piece

		return msg

	def recv_msg(self, bytes=0):
		while not bytes:
			bytes, = struct.unpack("!L", self.recv_bytes(4))

		return self.recv_bytes(bytes)

	def expect_reply(self, *seq):
		msg = ''.join(seq_generator(*seq))
		inp = self.get_msg()
		if inp[0] == CTL:
			if inp[1] != msg:
				raise RuntimeError("expected %s got %s" % (msg, inp[1]))
		if inp[0] == SET:
			if inp[1] != 1 or inp[2] != msg:
				raise RuntimeError("expected 1 %s got %d %s" % (msg, inp[1], inp[2]))

	def send(self, msg, pos=0):
		while pos < len(msg):
			sent = socket.socket.send(self, msg[pos:])
			if not sent:
				raise RuntimeError("Connection dropped")

			pos += sent

	def get_msg(self):
		msg = self.recv_msg()
		c, = struct.unpack("!B", msg[0])
		fmt = msg_format[c]
		bytes = struct.calcsize(fmt)
		head, data = msg[:bytes], msg[bytes:]

		fields = struct.unpack(fmt, head)
		if data:
			fields += (data,)
		return fields

	def __exit__(self, *excinfo):
		self.close()


