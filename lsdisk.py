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


class disk_archives(dict):
	def __init__(self, diskpath):
		dict.__init__(self)

		self.diskpath = diskpath
		for name in os.listdir(diskpath):
			if name.startswith("ar-"):
				self[name] = tuple(self.read_super(name))

	def read_super(self, ar):
		super = os.path.join(self.diskpath, ar, "archive-super.tsv")
		if os.path.isfile(super):
			f = open(super)
			magic = f.readline()
			if magic == " - typherno archive -\n":
				id = f.readline()
				if id.startswith("v0/"):
					id = id[3:]
				yield id.split('/')[0]
				yield f.readline().strip("()\n")
			f.close()

	def bin_segments(self, ar):
		for name in os.listdir(os.path.join(self.diskpath, ar)):
			if name.endswith(".bin") and not name.endswith("-pieces.bin"):
				yield name

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
				print "  %s\t%s" % info, '.' * len(tuple(disk.bin_segments(info[0])))
		print ''
		sys.exit()

	print "--- %s ---" % disk.diskpath
	d = disk.ar_listing()
	for fs in d.keys():
		print fs
		for info in d[fs]:
			print "  %s\t%s" % info, '.' * len(tuple(disk.bin_segments(info[0])))
	unknown = tuple(disk.ar_unknown())
	if unknown:
		print "<unknown>"
		for ar in unknown:
			print "  %s" % ar, '.' * len(tuple(disk.bin_segments(ar)))
	print ''


