\copy (SELECT "idxnr","countnr","timedesc" FROM public.testdataidx WHERE idxnr >= 5000 AND idxnr < 10000) to 'testdata_with.csv' (FORMAT 'csv', delimiter ',');
