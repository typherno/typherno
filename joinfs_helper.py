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
import uuid

from typherno_common import page_opener


def already_subscribed(disk, ar):
	for line in page_opener(host, 4352, "Subscribers/%s" % ar):
		if line.split('\t')[0] == disk:
			return True
	return False


def new_accumulators(disk, tracker):
	for entry in page_opener(tracker, 4352, "Accumulators"):
		line = entry.split('\t')
		if line[5] == "0/0" and not already_subscribed(disk, line[0]):
			hostport = line[-1]
			if ':' in hostport:
				yield hostport
			else:
				yield "%s:9825" % tracker


def alldisks(path):
	for f in os.listdir(path):
		if f.startswith("disk-") and os.path.isdir(path):
			yield f


host, path = sys.argv[1:3]
if host == "localhost":
	host = "127.0.0.1"
path = os.path.abspath(path)
name = os.path.basename(path)
if not name.startswith("disk-"):
	disks = [d for d in alldisks(path)]
	if len(disks) > 1:
		sys.stderr.write("Ambiguous disk path, multiple candidates found:\n")
		for d in disks:
			sys.stderr.write("  %s\n" % d)
		sys.exit()
	if disks:
		name = disks[0]
		path = os.path.join(path, name)
	else:
		name = "disk-%s" % uuid.uuid4()
		path = os.path.join(path, name)
		sys.stderr.write("Creating disk %s\n" % path)
		os.mkdir(path)
		os.mkdir(os.path.join(path, "logs"))

sys.stdout.write(''.join(["%s %s\n" % (ar, path) for ar in new_accumulators(name[5:], host)]))

