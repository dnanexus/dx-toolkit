
def reverse_complement(seq):
    rc = {"A":"T", "T":"A", "G":"C", "C":"G", "a":"T", "t":"A", "c":"G", "g":"C"}
    result = ''
    for x in seq[::-1]:
        result += rc.get(x, x)
    return result
