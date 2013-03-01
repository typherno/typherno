# Copyright 2012-2013 Nate Diller
# All rights reserved

# not for distribution

import os
import sys

from typherno_common import page_opener

def mkdisk(path, fsname):
	fspath = os.path.join(path, fsname)
	if not os.path.exists(fspath):
		os.mkdir(fspath)

	recpath = os.path.join(path, fsname, "archive.tsv")
	if not os.path.exists(recpath):
		open(recpath, 'a').close()


def subscriber_line(line):
	hostport = line[-1]
	if ':' in hostport:
		return hostport
	else:
		return "%s:9825" % host


def already_subscribed(disk, ar):
	for line in page_opener(host, 4352, "Subscribers/%s" % ar):
		if line.split('\t')[0] == disk:
			return True
	return False


host, path = sys.argv[1:3]
if host == "localhost":
	host = "127.0.0.1"
path = os.path.abspath(path)
name = os.path.basename(path)
if not name.startswith("disk-"):
	sys.exit()

lines = [line for line in page_opener(host, 4352, "Accumulators")]
fsname = "ar-%s" % lines[0].split('\t')[0]
mkdisk(path, fsname)

tlines = [line.split('\t') for line in lines]
disk = name[5:]
arlist = [subscriber_line(line) for line in tlines if line[5] == "0/0" and not already_subscribed(disk, line[0])]
sys.stdout.write(''.join(["%s %s\n" % (ar, path) for ar in arlist]))

