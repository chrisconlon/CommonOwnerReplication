# Common Ownership in America: 1980-2017
Backus, Conlon and Sinkinson (2020)
AEJMicro-2019-0389
openicpsr-120083
A copy of the paper is here: https://chrisconlon.github.io/site/common_owner.pdf

## Before running code
To download the repo simply type:

    git clone https://github.com/chrisconlon/CommonOwnerReplication

You will need to download the following files (too large for GitHub) and place them in data/public:
1. out_scrape.parquet: Scraped 13F Filings for all high market cap firms

You can download this directly from https://www.dropbox.com/s/wsoksbzg4tis90h/out_scrape.parquet?dl=0

Please see https://sites.google.com/view/msinkinson/research/common-ownership-data for  more information.

### Dataset Size and Memory
1. We recommend that you have at least 64GB of RAM available.
2. All of the datasets saved will take up about 14 GB of drive space.
3. NumPy is used extensively for the calculations and is multithreaded (so more cores will help).
4. The computation of the $\kappa_{fg}$ terms is parallelized quarter by quarter explicitly (so cores will help a lot here).
5. But most of the time spent is in merging and filtering data in pandas (more cores don't help much).
5. Total runtime on a 2015 iMac with 64GB of RAM is around 3 hours.
6. WRDS download time is about an hour (Depends on internet speed) and total download is > 10GB.

### Downloading WRDS
User must provide (a WRDS account). User will be prompted for WRDS username and password in file 1_Download_WRDS_Data.py.

## How to run code
Change to the directory containing this file and run "./run_all.sh" on the terminal. The code should take approximately 3-10 hours to run. Tables and figures will be produced as described below.

Windows Users: instead use "run_all.bat" from the command prompt.

## File of origin for tables and figures

| Table/Figure Number 	| Generating File			|
| ----------------------|-------------------------------------- |
| Table 1		| (by hand)				|
| Table 2		| (by hand)		 		|
| Table 3		| table3_variance_decomp.py        	|
| Table 4		| table4_kappa_correlations.py         	|
| Figure 1		| plots2_kappa_official.py		|
| Figure 2		| plots1_basic_descriptives.py		|
| Figure 3		| plots1_basic_descriptives.py 		|
| Figure 4		| plots1_basic_descriptives.py		|
| Figure 5		| plots3_big_three_four.py 		|
| Figure 6		| plots2_kappa_official.py 		|
| Figure 7		| plots2_kappa_official.py 		|
| Figure 8		| plots5_investor_similarity.py 	|
| Figure 9		| plots2_kappa_official.py 		|
| Figure 10		| plots11_profit_simulations.py 	|
| Figure 11		| plots11_profit_simulations.py 	|
| Figure 12		| plots9_blackrock_vanguard.py	 	|
| Figure 13		| plots2_kappa_official.py 		|
| Figure 14		| plots2_kappa_official.py 		|
| Figure 15		| plots2_kappa_official.py 		|
| Figure 16		| plots5_airlines_cereal.py	 	|
| Figure 17		| plots6_sole_vs_shared.py 		|
| Figure A1		| plots1_basic_descriptives.py 		|
| Figure A2		| plots8_individual_firm_coverage.py 	|
| Figure A3		| plots10_kappa_comparison_appendix.py 	|
| Figure A4		| plots7_short_interest_coverage.py 	|
| Figure A5		| plots7_short_interest_coverage.py 	|
| Figure A6		| plots2_kappa_official.py 		|
| Figure A7		| plots2_kappa_official.py 		|
| Figure A8		| plots5_investor_similarity.py 	|



## Within-File Dependencies:
1_Download_WRDS_Data.py: 
    
    wrds_downloads

2_Process_WRDS_Data.py
    
    wrds_cleaning
    wrds_checks

3_Calculate_Kappas.py
     
    kappas
    investors
    firminfo
    utilities/quantiles import weighted_quantile


plots3_big_three_four.py: 

    kappas
    investors

plots5_airlines_cereal.py: 

    kappas import

plots9_blackrock_vanguard.py: 

    kappas import beta_to_kappa_merger_breakup

plots10_kappa_comparison_appendix.py: 

    utilities.matlab_util

## Python  dependencies
Python (version 3.4 or above) - install dependencies with 

    pip3 install -r requirements.txt

    numpy, pandas, matplotlib, pyarrow, brotli, seaborn, wrds, scikit-learn, pyhdfe, pyblp, statsmodels


## Files Provided

data/public:

1. manager_consolidations.csv: lists consolidated manager numbers: several manager actually correspond to one
2. permno_drops.csv: lists dropped permno IDs with reasons why they are dropped
3. big4.csv: lists manager Numbers for Blackrock, Fidelity, State Street, and Vanguard
4. DLE_markups_fig_v2.csv: markups from DeLoecker Eeckhout Unger (QJE 2020)
5. cereal.parquet: Scraped 13F Filings for firms within the cereal industry
6. airlines.parquet: Scraped 13F Filings for firms within the airline industry

Plus put the out_scrape.parquet downloaded file in this directory.

