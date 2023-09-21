from mrjob.job import MRJob
import re

TEXT_RE = re.compile(r"[\w]+")

class Task3(MRJob):

    def mapper (self, _, line):
     text = TEXT_RE.findall(line)

     for text1, text2 in zip(text, text[1:]):
        result = f"{text1}-{text2}"
        yield (result, 1)

    def combiner(self, result, counts):
       yield (result, sum(counts))

    def reducer(self, result, counts):
       yield(result, sum(counts))

if __name__ == '__main__':
   Task3.run()

