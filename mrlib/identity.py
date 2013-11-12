# mrlib.identity
# Identity Mapper and Reducer
#
# Author:   Benjamin Bengfort <ben@cobrain.com>
# Created:  Tue Nov 12 07:10:36 2013 -0500
#
# Copyright (C) 2013 Cobrain Company
# For license information, see LICENSE.txt
#
# ID: identity.py [] ben@cobrain.com $

"""
Classes that implement the Identity Map/Reduce Pattern
"""

##########################################################################
## Mappers and Reducers
##########################################################################

class IdentityMapper(object):
    """
    The identity function where the input key/value pair is sent as the
    output, essentially a pass through Mapper.

    Usage: When performing a reduce-side computation or attempting to
        leverage the sorting and shuffling feature.
    """

    def __call__(self, key, value):
        yield (key, value)

class IdentityReducer(object):
    """
    The identity reducer ensures that the same output from the mapper is
    sent to the output. To this end, the reducer must iterate through the
    values and yield key/value pairs exactly as the mapper does.

    Usage: When no reduce compuatation is needed, but the job does require
        shuffle and sort (e.g. instead of a 0 reducer job.)
    """

    def __call__(self, key, values):
        for value in values:
            yield (key, value)
