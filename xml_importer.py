# Aggregation and shit

# 1.Iterate words and add tags
# 2. Find the sentence before a specifically tagged word

import glob
from os import path

from data import Word, Sentence

from xml_loader import XMLLoader
from oracle_db_helper import OracleDbHelper

files = glob.glob(path.join('xml_files', '*.xml'))

total = len(files)
i = 0
for f in files:
    l = XMLLoader(f)
    l.load()

    if i % 10 == 0:
        print "completed: %d/%d" % (i, total)
        OracleDbHelper.instance().commit()


    i += 1

# When done, commit to the DB
OracleDbHelper.instance().commit()

