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

import uuid	# v2.5


path = os.path.join(os.path.abspath(sys.argv[1]), "disk-%s" % uuid.uuid4())
print "creating disk %s" % path
os.mkdir(path)
os.mkdir(os.path.join(path, "logs"))

