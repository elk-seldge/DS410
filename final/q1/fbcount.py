
from mrjob.job import MRJob

class FBCount(MRJob):
    def mapper(self):
       self.cache = {}
       self.LIMIT = 100

    def mapper(self , key , line):
       (left , right) = line.split(" ")
            if int(right) > 500:
               if !self.cache[left]:
                   self.cache_valid()
                   self.cache[left] = 1
               else:
                   self.cache_valid()
                   self.cache[left] += 1

    def mapper_final(self):
        if len(self.cache) != 0:
            for _ in self.cache:
                yield _, self.cache[_]
            self.cache.clear()

    def reducer(self , key , values):
       left = key
       myval = sum(values)
       if myval > 2:
          yield (left , myval +1)

    def cache_valid(self):
        if len(self.cache) > self.LIMIT:
            for _ in self.cache:
                yield _, self.cache[_]
            self.cache.clear()

if __name__ == ’__main__ ’:
    FBCount.run()
