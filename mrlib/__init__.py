# mrlib
# A library of classes to use with Dumbo for standard MR Design Patterns
#
# Author:   Benjamin Bengfort <ben@cobrain.com>
# Created:  Tue Nov 12 07:08:20 2013 -0500
#
# Copyright (C) 2013 Cobrain Company
# For license information, see LICENSE.txt
#
# ID: __init__.py [] ben@cobrain.com $

"""
Dumbo provides a few generic Mappers and Reducers implemented as functions,
The purpose of this module is to write a few canonical Map and Reduce
patterns as classes so that they can be reused or extended.
"""

##########################################################################
## Imports
##########################################################################

from identity import *
from statistics import *
