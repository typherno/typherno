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

import os
import sys
import random
import time

from typherno_common import *
import libmsg


def get_posix_attrs(localfd):
	from pwd import getpwuid
	from grp import getgrgid

	stat = os.fstat(localfd.fileno())

	mtime = time_stamp(stat.st_mtime)
	subsec = repr(stat.st_mtime).split('.')[1]
	user = getpwuid(stat.st_uid).pw_name
	group = getgrgid(stat.st_gid).gr_name

	return "%s.%s %s/%s" % (mtime, subsec, user, group)


class reporter_socket(libmsg.msg_socket):
	def __init__(self, uuid, prefix, bufsize=256*1024):
		libmsg.msg_socket.__init__(self)

		self.uuid = uuid
		self.role = "File Reporter"

		self.prefix = prefix
		self.bufsize = bufsize

		self.total_bytes = 0
		self.len_bytes = 0

	def tell(self):
		return self.len_bytes

	def handle_connect(self):
		self.send(ctl_msg(self.uuid, self.role, "Info"))

		x = None
		self._print("--- %s Status ---" % self.peer_role)
		for s in self.get_ctl_seq()[1:]:
			if x:
				self._print("  %s:\t%s" % (s, x))
				x = None
			else:
				x = s

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
		self.total_bytes += len(data)

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

		libmsg.msg_socket.close(self)

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
		starttime = time.time()
		for path in pathlist:
			try:
				self.write_path(path)
			except Exception, err:
				self._print(err)
		self.close()
		elapsed = time.time() - starttime
		bytes = self.total_bytes
		if elapsed:
			rate = float(bytes) / elapsed
			self._print("Closed connection, uploaded %s in %d seconds (%s/s)" % (mb_str(bytes), elapsed, mb_str(rate, 3)))
		else:
			self._print("Closed connection, uploaded %s" % mb_str(bytes))



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

