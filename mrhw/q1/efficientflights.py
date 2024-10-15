from mrjob.job import MRJob

# ITIN_ID,  YEAR,   QUARTER     ,ORIGIN             ,ORIGIN_STATE_NM,   DEST,   DEST_STATE_NM, PASSENGERS
# 2021118,  2021,   1           #CAE                ,South Carolina     #FLL,   Florida       #1.00

class EfficientFlights(MRJob):
    # we are going to have tuples like this (org/dest, (pssgnr_in, pssgnr_out)),, merge part of them when the cache is full, merge the same airport at last
    def mapper_init(self):
        self.cache = {}
        self.LIMIT = 100

    def mapper(self, key, line):
        _, _, _, org, _, dest, _, pssgnr = line.split("")
        try:
            pssgnr = float(pssgnr)
        except:
            return
        org1 = self.cache.get(org, (0,0))
        dest1 = self.cache.get(dest, (0,0))

        self.cache[org] = self.pssgnr_adder(org1, (pssgnr, 0))
        self.cache_valid()
        self.cache[dest] = self.pssgnr_adder(dest1, (0, pssgnr))
        self.cache_valid()

    def mapper_final(self):
        if len(self.cache) != 0:
            for _ in self.cache:
                yield _, self.cache[_]
            self.cache.clear()

    def reducer(self, key, values):
        arrv = 0
        dprt = 0
        for dprt1, arrv1 in values:
            arrv += arrv1
            dprt += dprt1
        yield key, "arrives:{}, leave:{}".format(arrv, dprt)

    def pssgnr_adder(self, item1, item2): # org: out, dest: in
        org_out, dest_in = item1
        dest_out, org_in = item2
        return (org_out+dest_out, org_in+dest_in)

    def cache_valid(self):
        if len(self.cache) > self.LIMIT:
            for _ in self.cache:
                yield _, self.cache[_]
            self.cache.clear()

if __name__ == "__main__":
    EfficientFlights.run()
