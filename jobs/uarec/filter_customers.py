# filter_customers
# Filter the orders based on a customer input file.
#
# Author:   Benjamin Bengfort <ben@cobrain.com>
# Created:  Tue Nov 12 13:08:48 2013 -0500
#
# Copyright (C) 2013 Cobrain Company
# For license information, see LICENSE.txt
#
# ID: filter_customers.py [] ben@cobrain.com $

"""
Filter the orders based on a customer input file.
"""

##########################################################################
## Imports
##########################################################################

from dumbo import main, Error, identityreducer

class FilterMapper(object):

    def __init__(self):
        with open(self.params['customers'], 'r') as customers:
            self.customers = set(int(line.strip()) for line in customers)

    def __call__(self, key, value):
        parts = value.strip().split('\t')
        try:
            cid = int(parts[4])
            if cid in self.customers:
                yield (cid, tuple(parts))
        except:
            pass

class WriteReducer(object):

    def __call__(self, key, values):
        for value in values:
            yield (value, '')

def runner(job):
    job.additer(FilterMapper, WriteReducer)

def starter(program):
    customers = program.delopt("customers")
    if not customers:
        raise Error("Must specify a customers file with -customers")

    program.addopt("param", "customers=" + customers)
    program.addopt("outputformat", "text")

if __name__ == '__main__':
    main(runner, starter)
