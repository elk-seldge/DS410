from mrjob.job import MRJob

class items(MRJob):

    def mapper_init(self):
        self.cache = {}
        self.LIMIT = 100

    def mapper(self, key, line):
        _, stkcd, _, qntty, _, _, _, cnty = line.split("\t")
        try:
            float(qntty)
        except:
            return

        if stkcd in self.cache:
            self.cache[stkcd][cnty] = qntty # {stkcd1:{cnty:qntty, ...}}
        else:
            self.cache[stkcd] = {cnty: qntty}

        self.cache_valid()

    def mapper_final(self):
        if len(self.cache) != 0:
            for stkcd in self.cache:
                for cnty in self.cache[stkcd]:
                    qntty = self.cache[stkcd][cnty]
                    yield stkcd, (qntty, cnty)
            self.cache.clear()

    def reducer(self, key, vals): # put all items all in dict, and find out which cnty appears most frequently
        amnt = 0
        cnty_qntty = {} # in this form {country: amount}
        for qntty, cnty in vals:
            amnt = amnt + qntty
            cnty_qntty[cnty] = cnty_qntty.get(cnty, 0) + qntty

        def find_most_popular(cq_dict): # have to be inside
            rec = ()
            largest = 0.0
            rtn = ""
            for key, value in cq_dict.items():
                rec = (key, value)
                if rec[1] > largest:
                    largest = rec[1]
                    rtn = rec[0]
            return rtn

        yield key, "(amount: {}, numcountries: {}, mostpopular: {})".format(amnt, len(cnty_qntty), (find_most_popular(cnty_qntty)))

    def cache_valid(self):
        if len(self.cache) > self.LIMIT:
            for stkcd in self.cache:
                for cnty in self.cache[stkcd]:
                    qntty = self.cache[stkcd][cnty]
                    yield stkcd, (qntty, cnty)
            self.cache.clear()

if __name__ == "__main__": 
    items.run()