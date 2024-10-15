# InvoiceNo	StockCode   Description	                        Quantity	InvoiceDate	    UnitPrice	CustomerID	Country
# 536365    #85123A	    WHITE HANGING HEART T-LIGHT HOLDER  #6.0	    12/1/2010 8:26	2.55	    17850	    ##United Kingdom
# 536365	71053	    WHITE METAL LANTERN	                6.0	        12/1/2010 8:26	3.39	    17850	    United Kingdom
# 536365	84406B	    CREAM CUPID HEARTS COAT HANGER	    8.0	        12/1/2010 8:26	2.75	    17850	    United Kingdom
# 536365	84029G	    KNITTED UNION FLAG HOT WATER BOTTLE	6.0	        12/1/2010 8:26	3.39	    17850	    United Kingdom
from mrjob.job import MRJob

class items(MRJob):

    def mapper(self, key, line):
        _, stkcd, _, qntty, _, _, _, cnty = line.split("\t")
        try:
            float(qntty)
        except:
            return
        yield stkcd, (qntty, cnty)

    def reducer(self, key, vals):
        amnt = 0
        cnty_qntty = {} # in this form {country: amount}
        for qntty, cnty in vals:
            amnt = amnt + qntty
            cnty_qntty[cnty] = cnty_qntty.get(cnty, 0) + qntty

        def find_most_popular(cq_dict):
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

if __name__ == "__main__": 
    items.run()
