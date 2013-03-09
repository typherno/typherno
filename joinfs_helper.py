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


host, path = sys.argv[1:3]
if host == "localhost":
	host = "127.0.0.1"
path = os.path.abspath(path)
name = os.path.basename(path)
if not name.startswith("disk-"):
	sys.stderr.write("No disk found at %s" % path)
	sys.exit()

sys.stdout.write(''.join(["%s %s\n" % (ar, path) for ar in new_accumulators(name[5:], host)]))

