# Common Ownership in America: 1980-2017 - Backus, Conlon and Sinkinson


## Before running code
To download the repo simply type:

    git clone https://github.com/chrisconlon/CommonOwnerReplication

You will need to download the following files (too large for GitHub) and place them in data/public:
1. out_scrape.parquet: Scraped 13F Filings for all high market cap firms
2. cereal.parquet: Scraped 13F Filings for firms within the cereal industry
3. airlines.parquet: Scraped 13F Filings for firms within the airline industry

You can download those directly from https://www.google.com/url?q=https%3A%2F%2Fwww.dropbox.com%2Fs%2Fzr3upim0x8md4qa%2FArchive.zip%3Fdl%3D0&sa=D&sntz=1&usg=AFQjCNHatIc3bGr68gGCKovuu4fXrCNAMw

Please see https://sites.google.com/view/msinkinson/research/common-ownership-data for  more information.

### Dataset Size and Memory
1. We recommend that you have at least 64GB of RAM available.
2. All of the datasets saved will take up about 12 GB of drive space.
3. NumPy is used extensively for the calculations and is multithreaded (so more cores will help).
4. The computation of the $\kappa_{fg}$ terms is parallelized quarter by quarter explicitly (so cores will help a lot here).
5. Total runtime on a 2015 iMac with 64GB of RAM is around 5 hours.
6. WRDS download time is about an hour (Depends on internet speed) and total download is > 10GB.

### Downloading WRDS
User must provide (a WRDS account). User will be prompted for WRDS username and password in file 1_Download_WRDS_Data.py.

## How to run code
Change to the directory containing this file and run "./run_all.sh" on the terminal. The code should take approximately ten hours to run. Tables and figures will be produced as described below.

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
    
    from wrds_downloads import clean_wrds, get_names, get_crosswalk, get_fundamentals, get_short_interest, get_segments, get_msf, get_s34, 
	pandas, out

2_Process_WRDS_Data.py
    
    from wrds_cleaning import expand_names, make_cusip_list, construct_fundamentals, construct_bus_segments, consolidate_mgrs, filter_sp, compute_betas, add_drops, process_scraped, blackrock_fix, add_permno, add_stock_splits, dedup_s34, combine_betas
    from wrds_checks import check_bigbeta, check_s34, check_names, check_blackrock, check_s34_coverage, check_multiple_cusip, check_fundamental_coverage

3_Calculate_Kappas.py
     
    from kappas import process_beta, beta_to_kappa, kappa_in_out, calc_chhis, fix_scrape_cols
    from investors import compute_investor_info, calc_big4
    from utilities/quantiles import weighted_quantile


plots3_big_three_four.py: 

    from kappas import process_beta
    from investors import calc_big4

plots5_airlines_cereal.py: 

    from kappas import do_one_period
    from utilities.groupby import applyParallel

plots9_blackrock_vanguard.py: 

    from kappas import beta_to_kappa_merger_breakup

plots10_kappa_comparison_appendix.py: 

    from utilities.matlab_util import coalesce

## Python  dependencies
Python (version 3.4 or above) - install dependencies with 

    pip3 install -r requirements.txt

: numpy, pandas, matplotlib, pyarrow, brotli, seaborn, wrds, scikit-learn, pyhdfe, pyblp


## Files Provided

data/public:

1. manager_consolidations.csv: lists consolidated manager numbers: several manager actually correspond to one
2. permno_drops.csv: lists dropped permno IDs with reasons why they are dropped
3. big4.csv: lists manager Numbers for Blackrock, Fidelity, State Street, and Vanguard
4. DLE_markups_fig_v2.csv: markups from DeLoecker Eeckhout Unger (QJE 2020)

Plus put the three downloaded files in this directory.

