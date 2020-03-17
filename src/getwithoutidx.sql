\copy (SELECT "idxnr","countnr","timedesc" FROM public.testdata WHERE idxnr >= 5000 AND idxnr < 10000) to 'testdata_without.csv' (FORMAT 'csv', delimiter ',');
