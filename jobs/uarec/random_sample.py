# random_sample
# Creates a Random Sample from the UA dataset
#
# Author:   Benjamin Bengfort <ben@cobrain.com>
# Created:  Tue Nov 12 09:43:15 2013 -0500
#
# Copyright (C) 2013 Cobrain Company
# For license information, see LICENSE.txt
#
# ID: random_sample.py [] ben@cobrain.com $

"""
Creates a contiguous Random Sample from the UA dataset
"""

##########################################################################
## Imports
##########################################################################

import random
from dumbo import main, sumreducer, identityreducer

SAMPLE_SIZE = 10000

class CustomerMapper(object):

    def __call__(self, key, value):
        """
        Yield customer_id, 1
        """
        value = value.split('\t')
        try:
            yield int(value[4]), 1
        except ValueError:
            pass

class OrderFilterMapper(object):

    def __init__(self, split=100000):
        self.counter = 1
        self.lines   = 0
        self.split   = split

    def __call__(self, key, value):
        """
        Yield only customers with more than two orders.
        """
        if value > 3:
            self.lines += 1
            if self.lines % self.split == 0:
                self.counter += 1
            yield self.counter, key

class RandomReducer(object):

    def __call__(self, key, values):
        """
        Yields random results according to Sample Size.
        """
        for item in random.sample(list(values), SAMPLE_SIZE):
            yield (item,)

def runner(job):
    job.additer(CustomerMapper, sumreducer, combiner=sumreducer)
    job.additer(OrderFilterMapper, RandomReducer)

if __name__ == '__main__':
    main(runner)
