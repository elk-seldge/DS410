from mrjob.job import MRJob

class q1(MRJob):

    def mapper(self, key, line):
        _, year, _, org, _, _, dest_state, pssngr = line.split(",")
        key_0 = org+", "+dest_state
        if (year == 2021):
            yield (key_0, (pssngr, 0))
        else:
            yield (key_0, (0, pssngr))

    def reducer(self, key, vals):
        arpt_dic = {}
        for item in vals:
            if (item[1][0] != 0):
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
    q1.run()
