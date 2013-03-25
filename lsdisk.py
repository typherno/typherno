#
# Copyright (c) 2013 Nate Diller.
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

from typherno_common import mb_str


class disk_archives(dict):
	def __init__(self, diskpath):
		dict.__init__(self)

		self.diskpath = diskpath
		for name in os.listdir(diskpath):
			if name.startswith("ar-"):
				self[name] = self.read_super(name)

	def read_super(self, ar):
		ret = ()
		super = os.path.join(self.diskpath, ar, "archive-super.tsv")
		if os.path.isfile(super):
			f = open(super)
			magic = f.readline()
			if magic == " - typherno archive -\n":
				id = f.readline()
				if id.startswith("v0/"):
					id = id[3:]
				fs = id.split('/')[0]
				ctime = f.readline().strip("()\n")
				ret = (fs, ctime)
			f.close()
		return ret

	def bin_segments(self, ar):
		for name in os.listdir(os.path.join(self.diskpath, ar)):
			if name.endswith(".bin") and not name.endswith("-pieces.bin"):
				yield os.path.join(self.diskpath, ar, name)

	def ar_unknown(self):
		for ar in self.keys():
			if not self[ar]:
				yield ar

	def fs_archives(self, fs):
		for ar in self.keys():
			info = self[ar]
			if info and info[0] == fs:
				yield (ar, info[1])

	def fs_listing(self, fs):
		ars = list(self.fs_archives(fs))
		ars.sort(key=lambda x: x[1])
		ars.reverse()
		return ars

	def ordered_ar(self, fs):
		return [x[0] for x in self.fs_listing(fs)]

	def ar_listing(self):
		d = {}
		for ar in self.keys():
			info = self[ar]
			if info:
				d.setdefault(info[0], []).append((ar, info[1]))
		for v in d.values():
			v.sort(key=lambda x: x[1])
		return d



if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "lsdisk.py /path/to/disk-[uuid]"
		sys.exit()

	disk = disk_archives(sys.argv[1])
	if len(sys.argv) > 2:
		for fsname in sys.argv[2:]:
			print "--- %s ---" % fsname
			for info in disk.fs_listing(fsname):
				bytes = 0
				segs = 0
				for s in disk.bin_segments(info[0]):
					bytes += os.path.getsize(s)
					segs += 1
				print "  %s\t%s" % info, "%6s" % mb_str(bytes, 1), '\t', '.' * segs
		print ''
		sys.exit()

	print "--- %s ---" % disk.diskpath
	d = disk.ar_listing()
	for fs in d.keys():
		print fs
		for info in d[fs]:
			bytes = 0
			segs = 0
			for s in disk.bin_segments(info[0]):
				bytes += os.path.getsize(s)
				segs += 1
			print "  %s\t%s" % info, "%6s" % mb_str(bytes, 1), '\t', '.' * segs
	unknown = tuple(disk.ar_unknown())
	if unknown:
		print "<unknown>"
		for ar in unknown:
			bytes = 0
			segs = 0
			for s in disk.bin_segments(ar):
				bytes += os.path.getsize(s)
				segs += 1
			print "  %s" % ar, "%6s" % mb_str(bytes, 1), '\t', '.' * segs
	print ''


