# Copyright 2012-2013 Nate Diller
# All rights reserved

# not for distribution

import socket
import struct
import os
import time

from typherno_common import *
from file_reporter import msg_socket

import platform		# v2.3
try:
	import ctypes	# v2.5
except:
	ctypes = None



class subscriber_socket(msg_socket):
	def __init__(self, path, uuid, provider, raw_cap, avail_cap):
		msg_socket.__init__(self)

		self.path = path
		self.uuid = uuid
		self.provider = provider
		self.raw_cap = raw_cap
		self.avail_cap = avail_cap

		self.role = "Subscriber"

		self.handles = {}

	def connected(self):
		p = self.getsockname()[1]
		t = time.strftime("%Y%m%d%H%M%S", time.localtime())

		self.log_file = open(os.path.join(self.path, "logs", "%s-%d.log" % (t, p)), 'a')
		self._print("Connected to %s:%d at %s" % (self.getpeername() + (t,)))

	def handle_connect(self):
		self.path = os.path.join(self.path, "ar-%s" % self.peer_info["File System"])
		cap = self.raw_cap

		self._print("Subcribing: %s capacity" % mb_str(cap))
		self.send(ctl_msg(self.uuid, self.role, "Info", str(cap), "Raw Capacity"))

	def status(self, ctl_str):
		cap = self.avail_cap
		self._print("Status: %s available" % mb_str(cap))
		self.send(ctl_msg("Status", str(cap), "Available Capacity", self.provider, "Provider"))

	def provision(self, channel, offset, name):
		if channel in self.handles:
			raise RuntimeError("Ch %d - already provisioned" % channel)
		if os.sep != '/':
			name = name.replace('/', os.sep)

		self._print("Ch %d - provisioning %s:%d" % (channel, name, offset))
		f = open(os.path.join(self.path, name), 'ab')
		f.seek(0, 2)
		if f.tell() != offset:
			raise RuntimeError("Size %d mismatch in %s" % f.tell(), name)

		self.handles[channel] = f

	def data(self, channel, data=''):
		handle = self.handles[channel]
		if data:
			handle.write(data)
			handle.flush()

		self._print("Ch %d - wrote %d" % (channel, len(data)))

	def data_ack(self, channel, cookie, data=''):
		handle = self.handles[channel]
		if data:
			handle.write(data)
			handle.flush()
		os.fsync(handle.fileno())

		self._print("Ch %d - wrote %d, replying Ack cookie %d" % (channel, len(data), cookie))
		self.send(ctl_msg(str(cookie), "Ack"))

	def reclaim(self, channel):
		if channel not in self.handles:
			raise RuntimeError("Ch %d - not provisioned" % channel)

		self.handles[channel].close()
		del self.handles[channel]

		self._print("Ch %d - reclaimed" % channel)

	handlers = { \
		CTL:	status, \

		PROVISION:	provision, \
		DATA:		data, \
		DATA_ACK:	data_ack, \
		RECLAIM:	reclaim, }

	def handler(self, msg_id, *args):
		self.handlers[msg_id](self, *args)

	def loop(self):
		while True:
			self.handler(*self.get_msg())

	def close(self):
		msg_socket.close(self)

		for f in self.handles.values():
			f.close()



def capacities(path):
	if platform.system() != 'Windows':
		fsstat = os.statvfs(path)
		b = fsstat.f_frsize
		return fsstat.f_blocks * b, fsstat.f_bavail * b

	raw_cap = ctypes.c_ulonglong(0)
	avail_cap = ctypes.c_ulonglong(0)
	ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(path), \
		ctypes.pointer(avail_cap), ctypes.pointer(raw_cap), None)

	return raw_cap.value, avail_cap.value


def main(host, port, path, uuid, provider, raw_cap, avail_cap):
	s = subscriber_socket(path, uuid, provider, raw_cap, avail_cap)
	try:
		try:
			s.connect((host, port))
			s.loop()
		except Exception, err:
			s._print(err)
			raise
	finally:
		s._print("Closing connection")
		s.close()


if __name__ == "__main__":
	import sys
	if len(sys.argv) < 3 or ':' not in sys.argv[1]:
		print "subscriber.py host:port path [provider]"
		sys.exit()

	dest, path = sys.argv[1:3]
	host, port = dest.split(':')

	path = os.path.normpath(path)
	name = os.path.basename(path)
	if not name.startswith("disk-"):
		print "No disk found at %s" % path
		sys.exit()
	raw_cap, avail_cap = capacities(path)
	if not avail_cap:
		print "Disk already full"
		sys.exit()

	provider = ''
	if len(sys.argv) > 3:
		provider = sys.argv[3]

	try:
		main(host, int(port), path, name[5:], provider, raw_cap, avail_cap)
	except Exception, err:
		pass

