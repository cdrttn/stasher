from math import *

def calc(max):
    return pow2(max+1)-1            

def pow2(n):
    return 1<<n

def log2(n):
    power = 31
    while n and not (n & 0x80000000):
        n <<= 1
        power -= 1

    return power

def findchunk(index, maxexp = 4):
    p = int(log(index, 2))
    if p <= maxexp:
        si = index - pow2(p)
    else:
        index = index - calc(maxexp) - 1
        dist = index / pow2(maxexp) 
        p = maxexp + 1 + dist
        si = index - pow2(maxexp) * dist
    return p, si

def findchunk2(index, maxexp = 4):
    #maximum index for exponential growth
    maxexpi = pow2(maxexp+1) - 1
    
    if index < maxexpi:
        p = log2(index + 1)
        si = index - pow2(p) + 1
    else:
        index -= maxexpi
        dist = index / pow2(maxexp) 
        p = maxexp + 1 + dist
        si = index - pow2(maxexp) * dist

    return p, si
