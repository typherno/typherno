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
	sys.stderr.write("No disk found at %s" % path)
	sys.exit()
disk = name[5:]

lines = [line.split('\t') for line in page_opener(host, 4352, "Accumulators")]
arlist = [subscriber_line(line) for line in lines if line[5] == "0/0" and not already_subscribed(disk, line[0])]
sys.stdout.write(''.join(["%s %s\n" % (ar, path) for ar in arlist]))

