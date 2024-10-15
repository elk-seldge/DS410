
from mrjob.job import MRJob

class q1(MRJob):
    def mapper_init(self):
        self.cache = {}
        self.LIMIT = 100

    def mapper(self, key, line):
        _, year, _, org, _, _, dest_state, pssngr = line.split(",")
        key_0 = org+", "+dest_state
        if len(self.cache) < self.LIMIT:
            if (year == 2021):
                if key_0 not in self.cache:
                    self.cache[key_0] = (pssngr, 0)
                else:
                    self.cache[key_0][0] += pssngr
            else:
                if key_0 not in self.cache:
                    self.cache[key_0] = (0, pssngr)
                else:
                    self.cache[key_0][1] += pssngr
        else:
            for item in self.cache:
                yield (item, self.cache[item])
            self.cache.clear()
            
    def mapper_final(self):
        if len(self.cache) != 0:
            for item in self.cache:
                yield (item, self.cache[item])
            self.cache.clear()

    def reducer(self, key, vals):
        arpt_dic = {}
        for item in vals:
            if (item[1][0] != 0): # this is 2021
               if item[0] not in arpt_dic.keys():
                   arpt_dic[item[0]] = [item[1][0], 0]
               else:
                   arpt_dic[item[0]][0] += item[1][0]
            else:
                if item[0] not in arpt_dic.keys():
                    arpt_dic[item[0]] = [0, item[1][1]]
                else:
                    arpt_dic[item[0]][1] += item[1][1]
        for key, val in arpt_dic.items():
            yield "{} [{}, {}]".format(key, val[0], val[1])

if __name__ == "__main__":
    q2.run()
