import pandas as pd
#import inflect
import csv
from datetime import datetime
import random

#p = inflect.engine()
VOLUME = 10000000  #10000000 creates a file of ca. 417 MB

def number_sequence(nint):
    """
    Generator for test data
    my test data has three columns : int, int, date
    """
    num = 0
    while num<nint:
        yield (random.randint(0,1000000), random.randint(0,1000000), datetime.today())
        num += 1

#testdata = {'index': [x for x in range(VOLUME)], 'name': ['Zahl '+str(x) for x in range(VOLUME)]}
#pd.DataFrame(data=testdata).to_csv('/home/jstrebel/devel/pyspark-test/testdata.csv', index=False, header=False, quoting=csv.QUOTE_NONNUMERIC)

numgen = number_sequence(VOLUME)
with open('../testdata_buf.csv', 'w', newline='') as csvfile:
    datawriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    datawriter.writerows(numgen)

