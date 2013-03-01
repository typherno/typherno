# Copyright 2013 Nate Diller
# All rights reserved

# not for distribution

import os
import sys

import uuid	# v2.5


path = os.path.join(os.path.abspath(sys.argv[1]), "disk-%s" % uuid.uuid4())
print "creating disk %s" % path
os.mkdir(path)
os.mkdir(os.path.join(path, "logs"))

