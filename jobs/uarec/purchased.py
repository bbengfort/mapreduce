#!/usr/bin/env python

class Mapper(object):

    def __call__(self, key, value):
        value = value.split('\t')
        if len(value) > 1:
            yield value[1], value[0]

class Reducer(object):

    def __call__(self, key, values):
        dist = {}
        for item in values:
            if item in dist:
                dist[item] += 1
            else:
                dist[item] = 1
        yield key, tuple(dist.items())

def runner(job):
    job.additer(Mapper, Reducer)

def starter(prog):

    params = ("stopwords", "dedup")
    for param in params:
        arg = prog.delopt(param)
        if arg: prog.addopt("param", "%s=%s" % (param, arg))

if __name__ == "__main__":
    import dumbo
    dumbo.main(runner, starter)
