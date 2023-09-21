from mrjob.job import MRJob
import re

TEXT_RE = re.compile(r"[\w]+")

class CountWords(MRJob):

    def mapper(self, _, line):
        lines = TEXT_RE.findall(line)
        for text in lines:
            yield text.lower(), 1

    
    def combiner(self, text, counts):
        yield text, sum(counts)

    def reducer(self, text, counts):
        yield text, sum(counts)

if __name__ == '__main__':
    CountWords.run()