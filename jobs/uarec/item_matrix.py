#!/usr/bin/env python

from copy import copy

class OrderMapper(object):

    def __call__(self, key, value):
        """
        yields custid, (sku, 1)
        """
        value = value.split('\t')
        if len(value) > 4:
            yield value[4], (value[0], 1)

class ItemMatrixMapper(object):

    def __call__(self, key, value):
        """
        yields sku, tuple((sku, count))
        """
        if len(value) > 1:
            for idxs, sku in enumerate(value):
                for idxi, item in enumerate(value):
                    if idxi != idxs:
                        yield sku[0], item

class StripeReducer(object):

    def __call__(self, key, values):
        """
        yields key, tuple((valid, count))
        """
        dist = {}
        for item, count in values:
            if item in dist:
                dist[item] += count
            else:
                dist[item] = count
        yield key, tuple(dist.items())

def runner(job):
    job.additer(OrderMapper, StripeReducer)
    job.additer(ItemMatrixMapper, StripeReducer)

def starter(prog):

    params = ("stopwords", "dedup")
    for param in params:
        arg = prog.delopt(param)
        if arg: prog.addopt("param", "%s=%s" % (param, arg))

if __name__ == "__main__":
    import dumbo
    dumbo.main(runner, starter)
