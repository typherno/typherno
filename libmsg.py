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

	def unpack_seq(self, chunks):
		while chunks:
			bytes, = struct.unpack("!B", self.do_chomp(chunks, 1))
			yield self.do_chomp(chunks, bytes)

	def get_ctl_seq(self):
		msg_id, chunks = self.get_msg()
		if not (msg_id == CTL):
			raise RuntimeError("Invalid server response, msg_id=%d" % msg_id)
		return [s for s in self.unpack_seq(chunks)]

	def parse_info(self):
		seq = self.get_ctl_seq()
		self.peer_uuid = seq.pop(0)
		self.peer_role = seq.pop(0)
		if seq.pop(0) != "Info":
			raise RuntimeError("info: invalid sequence")
		while seq:
			self.peer_info[seq.pop(0)] = seq.pop(0)

		self._print("--- %s Info ---" % self.peer_role)
		self._print("%s:\t%s" % (self.peer_role, self.peer_uuid))
		for k, v in self.peer_info.items():
			self._print("  %s:\t%s" % (k, v))

	def connected(self):
		pass

	def connect(self, *sockargs):
		socket.socket.connect(self, *sockargs)

		self.connected()
		self._print("Connected, sending handshake")
		self.send(HANDSHAKE)
		verify_handshake(self.recv_bytes(len(HANDSHAKE)))
		self._print("Remote handshake verified, waiting for info structure...")
		self.parse_info()

		self.handle_connect()

	def recv_chunks(self, bytes):
		count = 0
		while count < bytes:
			piece = self.recv(bytes - count)
			if not piece:
				raise RuntimeError("Connection dropped")
			yield piece
			count += len(piece)

	def recv_bytes(self, bytes):
		chunks = [s for s in self.recv_chunks(bytes)]
		if len(chunks) > 1:
			return ''.join(self.recv_chunks(bytes))
		return chunks[0]

	def recv_msg(self, bytes=0):
		while not bytes:
			bytes, = struct.unpack("!L", self.recv_bytes(4))
		return [s for s in self.recv_chunks(bytes)]

	def expect_reply(self, *seq):
		msg = ''.join(seq_generator(*seq))
		inp = self.get_msg()
		if inp[0] == CTL:
			if ''.join(inp[1]) != msg:
				raise RuntimeError("expected %s got %s" % (msg, repr(inp[1])))
		if inp[0] == SET:
			if inp[1] != 1 or ''.join(inp[2]) != msg:
				raise RuntimeError("expected 1 %s got %d %s" % (msg, inp[1], repr(inp[2])))

	def send(self, msg, pos=0):
		while pos < len(msg):
			sent = socket.socket.send(self, msg[pos:])
			if not sent:
				raise RuntimeError("Connection dropped")
			pos += sent

	def do_chomp(self, chunks, bytes):
		assert bytes > 0
		soup = []
		while bytes and len(chunks[0]) <= bytes:
			bytes -= len(chunks[0])
			soup.append(chunks.pop(0))
		if bytes:
			chunk = chunks[0]
			a, chunks[0] = chunk[:bytes], chunk[bytes:]
			soup.append(a)
		if len(soup) > 1:
			return ''.join(soup)
		return soup[0]

	def get_msg(self):
		chunks = self.recv_msg()
		c, = struct.unpack("!B", chunks[0][0])
		fmt = msg_format[c]
		bytes = struct.calcsize(fmt)
		head = self.do_chomp(chunks, bytes)
		fields = struct.unpack(fmt, head)
		return fields + (chunks,)

	def __exit__(self, *excinfo):
		self.close()


