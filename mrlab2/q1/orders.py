from mrjob.job import MRJob
import csv
class Orders(MRJob):  #MRJob version
    def mapper(self, key, line):
        parts = list(csv.reader([line]))[0]
        part = parts.strip("")
        countries = part[-1]
        prices = part[-3]
        amount = part[-5]
        cnt = 0
        for item in countries:
            yield (item, price[cnt] * amount[cnt])
            cnt = cnt + 1

    def reducer(self, key, values):
        yield (key, sum(values))
	
if __name__ == '__main__':
     Orders.run()
