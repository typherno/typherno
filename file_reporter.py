# Copyright 2012-2013 Nate Diller
# All rights reserved

# not for distribution

import socket
import struct
import os
import sys
import random

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



def get_posix_attrs(localfd):
	from pwd import getpwuid
	from grp import getgrgid

	stat = os.fstat(localfd.fileno())

	mtime = time_stamp(stat.st_mtime)
	subsec = repr(stat.st_mtime).split('.')[1]
	user = getpwuid(stat.st_uid).pw_name
	group = getgrgid(stat.st_gid).gr_name

	return "%s.%s %s/%s" % (mtime, subsec, user, group)



class reporter_socket(msg_socket):
	def __init__(self, uuid, prefix, bufsize=256*1024):
		msg_socket.__init__(self)

		self.uuid = uuid
		self.role = "File Reporter"

		self.prefix = prefix
		self.bufsize = bufsize

		self.len_bytes = 0

	def tell(self):
		return self.len_bytes

	def handle_connect(self):
		self.send(ctl_msg(self.uuid, self.role, "Info"))

		x = None
		self._print("--- %s Status ---" % self.peer_role)
		for s in unpack_seq(self.recv_msg()[1:])[1:]:
			if x:
				self._print("%s:\t%s" % (s, x))
				x = None
			else:
				x = s
		self._print('')

		self.send(cmd_msg(1, "Provision"))
		self.expect_reply("Ok")

	def file_size(self, size):
		self.send(cmd_msg(1, str(size), "File Size"))

	def create(self, path, attrs=''):
		self.len_bytes = 0

		self.send(raw_msg(path, ENTRY, 1))
		if attrs:
			self.send(cmd_msg(1, attrs, "File Attribute"))

	def append(self, data):
		self.send(raw_msg(data, DATA, 1))
		self.len_bytes += len(data)

	def cancel(self):
		self.send(cmd_msg(1, "Abort"))

	def commit(self):
		self.send(cmd_msg(1, "Commit"))

	def commit_sync(self):
		self.send(cmd_msg(1, "42", "Commit"))

		self.expect_reply("42", "Ack")

	def close(self):
		self._print("Syncing ...")
		self.send(cmd_msg(1, "Reclaim"))
		self.expect_reply("Ok")

		msg_socket.close(self)

	def convertpath(self, path):
		if os.sep != '/':
			path = path.replace(os.sep, '/')
		out = []
		for c in path.split('/'):
			if c == '.':
				pass
			elif c == "..":
				out and out.pop()
			else:
				c and out.append(c)
		return '/'.join([self.prefix] + out)

	def write_fd(self, fd, pathname):
		size = os.path.getsize(pathname)
		if os.sep != '/':
			pathname = pathname.replace(os.sep, '/')
		if not pathname.startswith('/'):
			pathname = "/%s" % pathname

		self.file_size(size)

		try:
			attrs = get_posix_attrs(fd)
		except:
			attrs = ''
		self.create(self.convertpath(pathname), attrs)
		self._print("Writing (%s) %s" % (mb_str(size), self.convertpath(pathname)))
		data = fd.read(self.bufsize)
		while data:
			self.append(data)
			data = fd.read(self.bufsize)
		self.commit()
		# self.commit_sync()

	def write_file(self, pathname):
		fd = None
		try:
			try:
				fd = open(pathname, 'rb')
			except Exception, err:
				self._print(err)
			else:
				self.write_fd(fd, pathname)
		finally:
			fd and fd.close()

	def write_path(self, path):
		if os.path.isdir(path):
			for cpath, dirs, files in os.walk(path):
				for name in files:
					self.write_file(os.path.join(cpath, name))
		else:
			self.write_file(path)

	def main(self, host, port, pathlist):
		self._print("Connecting to %s:%d ... " % (host, port))
		self.connect((host, port))
		for path in pathlist:
			try:
				self.write_path(path)
			except Exception, err:
				self._print(err)
		self.close()
		self._print("Closed connection to %s" % host)



def weighted_accumulators(host):
	for line in page_opener(host, 4352, "Upload"):
		items = line.split('\t')
		busy, width = items[1].strip('w').split('/')
		for i in range(int(width) - int(busy)):
			yield items[0]


def get_accumulator(host):
	candidates = [a for a in weighted_accumulators(host)]
	local_candidates = [a for a in candidates if a.startswith("127.0.0.1")]
	if local_candidates:
		return random.choice(local_candidates)
	if candidates:
		return random.choice(candidates)
	return ''


if __name__ == "__main__":
	import sys
	if len(sys.argv) < 3:
		print "file_reporter.py [uuid@]host[:port][prefix-dir] files | directories"
		sys.exit()

	dest = sys.argv[1]
	prefix = ''
	if '/' in dest:
		pos = dest.find('/')
		dest, prefix = dest[:pos], dest[pos:]
		prefix = '/'.join([''] + [c for c in prefix.split('/') if c])
	uuid = ''
	if '@' in dest:
		uuid, dest = dest.split('@')

	if ':' not in dest:
		dest = get_accumulator(dest)
		if not dest:
			print "Archive busy or no accumulators ready"
			sys.exit()
	host, port = dest.split(':')

	reporter = reporter_socket(uuid, prefix)
	try:
		reporter.main(host, int(port), sys.argv[2:])
	except Exception, err:
		print err

