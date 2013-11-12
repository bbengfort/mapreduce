# mrlib.statistics
# Libraries for implement basic statistical computations
#
# Author:   Benjamin Bengfort <ben@cobrain.com>
# Created:  Tue Nov 12 07:17:30 2013 -0500
#
# Copyright (C) 2013 Cobrain Company
# For license information, see LICENSE.txt
#
# ID: statistics.py [] ben@cobrain.com $

"""
Dumbo provides a few mappers and reducers for statistical computations
implemented as functions. These mappers and reducers are implemented here
as classes so that they can be subclassed and customized.

The classes Specified here are:

* SumReducer: compute sum for each key (can be used as combiner)
* SumsReducer: compute sums for multiple values for each key (can be used as combiner)
* NLargestReducer and NLargestCombiner:  compute n-largest items for key (max)
* NSmallestReducer and NSmallestCombiner: compute n-smallest items for key (min)
* MeanReducer and MeanCombiner: compute the arithmetic mean for each key

"""

##########################################################################
## Imports
##########################################################################

import heapq

from math import sqrt
from itertools import imap, izip, chain

##########################################################################
## Reducers and Combiners
##########################################################################

class SumReducer(object):
    """
    Sums the values for the particular key.

    Example Input:  key, (1, 3, 7, 15)
    Example Output: key, 26

    Can also be used as a combiner.
    """

    def __call__(self, key, values):
        yield (key, sum(values))

class SumsReducer(object):
    """
    Sums each record in each tuple of the values

    Example Input:  key, ((1,2), (1, 3), (1, 4))
    Example Output: key, (3, 9)

    Can also be used as a combiner.
    """

    def __call__(self, key, values):
        yield key(, tuple(imap(sum, izip(*values))))

class NLargestReducer(object):
    """
    Yields the n-largest values for the key. You can also specify a key
    function to the class that is used to extract a comparison key from
    each element in the iterable (e.g. str.lower)

        NLargestReducer(10, key=str.lower) is equivalent to:
        sorted(values, key=str.lower, reverse=True)[:n]

    For n=3

    Example Input:  key, ((10, 3, 5), (1, 11, 2), (4,))
    Example Output: key, (11, 10, 5)

    Use this class for MaxReducer (n=1), or top-ten filter (n-10), etc.

    NOTE: MUST BE USED WITH THE NLargestCombiner
    """

    def __init__(self, n, key=None):
        self.n   = n
        self.key = key

    def __call__(self, key, values):
        yield (key, heapq.nlargest(self.n, chain(*values), key=self.key))

class NLargestCombiner(NLargestReducer):
    """
    The NLargestReducer uses chain to treat consecutive iterables in the
    values as a single iterable passed to heapq. This is to ensure that
    when this combiner is used, the tuple of values that is output from
    each combiner will be correctly computed with the n-largest
    computation.

    When not using this combiner, you can simply use this class as the
    reducer, but it is still recommended to use the NLargestReducer with
    this NLargestCombiner.

    For n=3

    Example Input:  key, (10, 8, 3, 7, 3, 11)
    Example Output: key, (11, 10, 8)
    """

    def __call__(self, key, values):
        yield (key, heapq.nlargest(self.n, values, key=self.key))

class NSmallestReducer(object):
    """
    The opposite of the NLargestReducer - yields the n smallest values in
    the dataset specified by the values iterator. You can specify a key
    function that is used to extract a comparison key from each element in
    the iterable (e.g. str.lower).

        NSmallestReducer(10, key=str.lower) is equivalent to:
        sorted(values, key=str.lower, reverse=False)[:n]

    For n=3

    Example Input:  key, ((10, 3, 5), (1, 11, 2), (4,))
    Example Output: key, (1, 2, 3)

    Use this class for MinReducer (n=1), or bottom-ten filter (n-10), etc.

    NOTE: MUST BE USED WITH THE NSmallestCombiner
    """

    def __init__(self, n, key=None):
        self.n   = n
        self.key = key

    def __call__(self, key, values):
        yield (key, heapq.nsmallest(self.n, chain(*values), key=self.key))

class NSmallestCombiner(NSmallestReducer):
    """
    The NSmallestReducer uses chain to treat consecutive iterables in the
    values as a single iterable passed to heapq. This is to ensure that
    when this combiner is used, the tuple of values that is output from
    each combiner will be correctly computed with the n-smallest
    computation.

    When not using this combiner, you can simply use this class as the
    reducer, but it is still recommended to use the NSmallestReducer with
    this NSmallestCombiner.

    For n=3

    Example Input:  key, (10, 8, 3, 7, 3, 11)
    Example Output: key, (3, 7, 8)
    """

    def __call__(self, key, values):
        yield (key, heapq.nsmallest(self.n, values, key=self.key))

class MeanReducer(object):
    """
    Computes the arithmetic mean for a set of values that was output from
    the MeanCombiner, e.g. (sum, count) pairs.

    Example Input:  key ((1.2, 3), (2.3, 2), (4.2, 4), (6.8, 1))
    Example Output: key, 1.45

    NOTE: MUST BE USED WITH THE MeanCombiner
    """

    def __call__(self, key, values):
        total, count = imap(sum, izip(*values))
        mean = total / float(count) # Ensure float result by casting count
        yield (key, mean)

class MeanCombiner(object):
    """
    Combines the sums and counts of a Mapper to yield to the MeanReducer,
    the combiner does not compute an intermediary mean, which would lead
    to loss in precision, but does handle some of the interim summations.

    Example Input:  key, (1.2, 2.3, 4.8, 4.6, 1.2)
    Example Output: key, (14.1, 5)
    """

    def __call__(self, key, values):
        yield (key, (sum(values), len(values)))

class StatsReducer(object):
    """
    Computes a total statistics package for a given data set including the
    sample size, the sum, the mean, the standard deviation, and the min
    and max values. This is a handy shortcut for statistically evaluating
    a data set on a per key basis with ease.

    Example Input:  key, ((count, sum, sum**2, min, max), ...)
    Example Output: key, (n, mean, stddev, minimum, maximum)

    NOTE: MUST BE USED WITH THE StatsCombiner
    """

    def __call__(self, key, values):
        columns = izip(*values)
        s0 = sum(columns.next()) # n
        s1 = sum(columns.next()) # sum(x)
        s2 = sum(columns.next()) # sum(x**2)
        minimum = min(columns.next())
        maximum = max(columns.next())
        mean    = float(s1) / s0
        stddev  = 0
        if s0 > 1:
            stddev = sqrt((s2-s1**2/float(s0))/(s0-1)) # sample standard deviation
        yield (key, (s0, mean, stddev, minimum, maximum))

class StatsCombiner(object):
    """
    Combines the count, sum, sum**2, and minimum and maximium values for a
    set of values from the Mapper. This output is intended to be sent to
    the StatsReducer to compute the overall statistical set for the data.

    Example Input:  key, (4, 9, 2, 3, 4, 1, 0, 3)
    Example Output: key, (8, 26, 136, 0, 9)
    """

    def __call__(self, key, values):
        columns = izip(*((1, value, value**2, value, value) for value in values))
        s0 = sum(columns.next()) # n
        s1 = sum(columns.next()) # sum(x)
        s2 = sum(columns.next()) # sum(x**2)
        minimum = min(columns.next())
        maximum = max(columns.next())
        yield (key, (s0, s1, s2, minimum, maximum))
