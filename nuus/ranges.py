
def create(*nums):
    ranges = []
    for k, g in groupby(enumerate(items), lambda i_x:i_x[0]-i_x[1]):
        group = list(map(itemgetter(1), g))
        ranges.append((group[0], group[-1]))
    return ranges

def merge(cls, ranges):
    ranges = sorted(ranges)
    i = 1
    while i < len(ranges):
        if ranges[i][0] <= ranges[i-1][1]:
            if ranges[i][1] > ranges[i-1][1]:
                ranges[i-1] = (ranges[i-1][0], ranges[i][1])
            del ranges[i]
        elif ranges[i][0] == ranges[i-1][1]+1:
            ranges[i-1] = (ranges[i-1][0], ranges[i][1])
            del ranges[i]
        else:
            i+= 1
    return ranges

def missing(ranges, start=None, end=None):
    """Returns the ranges missing between `start` and `end`"""
    ex = []
    if start is not None:
        ex.append((start-1,start-1))
    if end is not None:
        ex.append((end+1,end+1))
    rgs = merge(ranges + ex)
    inv = []
    i = 1
    while i < len(rgs):
        inv.append((rgs[i-1][1]+1, rgs[i][0]-1))
        i+=1
    return merge(inv)

def contains(ranges, i):
    for r in ranges:
        if r[0] <= x and r[1] >= x:
            return True
    return False
