

def q1(mystring):
    """ split the string by tabs to get an array and return the array """
    str_rec = ""
    rtn_lst = []
    for chr in mystring:
        if chr != "\t":
            str_rec
    # return mystring.split("\t") # with builtin function


def q2(mystring):
    """ split the string by tabs to get an array and return the second element of the array """

    pass



def q3(myarray):
    """ myarray is an list of pairs. this function should return the sum of the first
    items in the pair and the sum of the second items """
    x_coord = 0
    y_coord = 0

    for item in myarray:
        x_coord += item[0]
        y_coord += item[1]
    
    return (x_coord, y_coord)


def q4(mystringarray):
    """ return the position of the first occurrence of the string 'hi' or -1 if it is not found.
    you cannot change how the array is iterated and you cannot use any list operations on mystringarray"""
    indx = -1
    for mystring in mystringarray:
        if "hi" in mystring:
            indx = mystringarray.index(mystring)
            break

    return indx


def q5(myarray):
    """ return a dictionary containing the counts of items in the input array """
    rtn_dic = {}
    for item in myarray:
        if item not in rtn_dic:
            rtn_dic[item] = 1
        else:
            rtn_dic[item] += 1

    return rtn_dic


