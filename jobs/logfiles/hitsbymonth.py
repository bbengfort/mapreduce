#!/usr/bin/env python

import re

logline = re.compile(r'^(local|remote) - - \[(.*)\] "(.*)" (\d+) (\d+)$', re.I)

def mapper(key, value):
    line = logline.match(value)
    if line:
        dt = line.groups()[1]
        yield dt.split('/')[1], 1

if __name__ == '__main__':
    import dumbo
    dumbo.run(mapper, dumbo.sumreducer, dumbo.sumreducer)
