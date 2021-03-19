# cmd-online-job-adverts

Script for tansforming online job adverts data.

Transform takes 1 input xlsx file, remove any previous xlsx files so there is no confusion in which spreadsheet is picked up. The input file is found from the ONS website https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/onlinejobadvertestimates 

Place the input file in the same directory as the script or alternatively change the location variable in transform.ipynb to match the file location.

2 output files are created (one for each edition)
- v4-job-advert-estimates-2019-index-by-category.csv
- v4-job-advert-estimates-feb-2020-index-by-category.csv

There is some sparsity within these datasets, which the SparsityFiller function takes care of.

The transform requires the use of databaker & databakerUtils.
