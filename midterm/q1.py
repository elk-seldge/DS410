from mrjob.job import MRJob

class q1(MRJob):

    def mapper(self, key, line):
       _, _, _, org, _, _, pssgnr = line.split("")
       try:
           float(pssgnr)
       except:
           return
       yield (org, (pssgnr1,pssgnr2 )) # (airport, (2021  outgoing, 2022 outgoing))

    def reducer(self, key, values):
       dprt = 0
       diff = 0
       arpt_dic = {}
       for arpt, outgoing in values:
           if arpt not in arpt_dic:
               arpt_dic[arpt] = (outgoing[0], outgoing[1])
           else:
               arpt_dic[arpt] = (arpt_dic[arpt][0] + outgoing[0], arpt_dic[arpt][1] +outgoing[1])
           dprt = dprt + sum(arpt_dic[arpt])
           diff = arpt_dic[arpt][0]-arpt_dic[1]
           
       yield key, ""{} [{}, {}]"".format(arpt, dprt, diff)
if __name__ == "__main__":
    q1.run()
