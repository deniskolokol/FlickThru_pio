#-- prepare file
import random
import os

filename = os.path.abspath('../data/full.csv')
src = open(filename, 'r+')
trg = open(os.path.join(os.path.dirname(filename), 'sample.csv'), 'w+')
added, nxt, lnum = 0, 0, 0
limit = 500
for line in src.readlines():
    if added >= limit:
        break

    if lnum == nxt:
        trg.write(line)
        added += 1
        nxt = lnum + random.randint(50, 100)

    lnum += 1

src.close()
trg.close()
#--
