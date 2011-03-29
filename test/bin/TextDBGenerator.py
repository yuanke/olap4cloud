import sys
import random

f = open('data.txt', 'w')
for i in xrange(100000000):
        f.write(str(i) + "\t" + str(random.randint(0,1000)) + "\n")
f.close()