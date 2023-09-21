from mrjob.job import MRJob
from nltk.corpus import stopwords
import re

TEXT_RE = re.compile(r"[\w]+")
set_stopwords = set(stopwords.words('english'))

class Task2 (MRJob):
    def mapper (self, _, line):
        lines = TEXT_RE.findall(line)
        for text in lines:
            if text in set_stopwords:
                continue
            yield text.lower(), 1

    def combiner (self, text, counts):
        yield text, sum(counts)

    def reducer (self, text, counts):
        yield text, sum(counts)

if __name__ == '__main__':
    Task2.run()