from mrjob.job import MRJob  

class wordcount(MRJob):

    def mapper(self, key, line): 
        words = line.split() 
        for w in words:
            yield (w, 1)

    def reducer_init(self):
        self.max_cnt = 0
        self.freqest_word = ''

    def reducer(self, key, vals):
        cnt = sum(vals)
        if cnt > self.max_cnt:
            self.max_cnt = cnt
            self.freqest_word = key
        yield (key, cnt)
        

    def reducer_final(self):
        yield "MostFrequent:\t {}".format(self.freqest_word)

if __name__ == "__main__": 
    wordcount.run()
