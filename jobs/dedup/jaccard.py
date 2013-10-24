
import nltk
import string

from nltk.tokenize import wordpunct_tokenize

class Mapper(object):

    def __init__(self):
        if 'stopwords' in self.params:
            with open(self.params['stopwords'], 'r') as excludes:
                self._stopwords = set(line.strip() for line in excludes)
        else:
            self._stopwords = None

        self.lemmatizer = WordNetLemmatizer()

    def __call__(self, key, value):
        pid, name = value.split('\t')[:2]
        yield self.exclude(token for token in self.tokenize(name)), pid

    @property
    def stopwords(self):
        if not self._stopwords:
            self._stopwords = nltk.corpus.stopwords.words('english')
        return self._stopwords

    def tokenize(self, sentence):
        sentence = sentence.decode('utf8')
        for token in wordpunct_tokenize(sentence):
            token = self.normalize(token)
            if token: yield token

    def normalize(self, token):
        token =  token.lower()
        if token in string.punctuation:
            return None
        return token

    def exclude(self, iterable):
        return filter(lambda x: x not in self.stopwords, iterable)

class Reducer(object):

    def __init__(self):
        self.dedup = ('dedup' in self.params)

    def __call__(self, key, values):
        if self.dedup:
            values = tuple(values)
            if len(values) > 1:
                yield tuple(key), values
        else:
            yield tuple(key), tuple(values)

    def jaccard(seta, setb):
        seta = set(seta)
        setb = set(setb)
        nint = len(seta.intersection(setb))
        return nint / float(len(seta) + len(setb) - nint)

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
