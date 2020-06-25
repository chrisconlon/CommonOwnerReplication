

from our_plot_config import raw_dir, wrds_dir, derived_dir, checks_dir
import pandas as pd

from wrds_cleaning import expand_names, make_cusip_list, construct_fundamentals 
from wrds_cleaning import construct_bus_segments, consolidate_mgrs, filter_sp
from wrds_cleaning import compute_betas, add_drops, process_scraped, blackrock_fix
from wrds_cleaning import  add_permno, add_stock_splits, dedup_s34, combine_betas

from wrds_checks import check_bigbeta, check_s34, check_names, check_blackrock
from wrds_checks import check_s34_coverage, check_multiple_cusip, check_fundamental_coverage


# Public (hand) inputs
f_scrape = raw_dir / 'out_scrape.parquet' ## CRM renamed
f_big4= raw_dir /'big4.csv'

# raw data pulls
# CRM update: calling these WRDS
f_raw_s34= wrds_dir /'raw_s34.parquet'
f_splist = wrds_dir  / 'sp500_list.parquet'
f_crsp_names = wrds_dir / 'crsp_names.parquet'
f_msf_data = wrds_dir / 'crsp_msf.parquet'
f_short = wrds_dir / 'short_interest.parquet'
f_fundamentals= wrds_dir / 'fundamentals_data.parquet'
f_segments = wrds_dir / 'wrds_segments.parquet'

# drops and consolidations
f_permno_drops = raw_dir / 'permno_drops.csv'
f_mgr_consolidations = raw_dir / 'manager_consolidations.csv'

## Outputs
# other info
f_comp_info = derived_dir / 'compustat_info.parquet'
f_names_expanded = derived_dir / 'expanded_names.parquet'

# Betas
f_betas_unfiltered = derived_dir / '13f_sp500_unfiltered.parquet'
f_betas_scraped = derived_dir / '13f_scraped.parquet'
f_frankenbetas = derived_dir / '13f_sp500_frankenbeta.parquet'

# Read in the raw parquet files from SQL queries
df_sp500 = pd.read_parquet(f_splist)
df_names = pd.read_parquet(f_crsp_names)
df_msf2  = pd.read_parquet(f_msf_data)
df_short = pd.read_parquet(f_short)
raw_s34  = pd.read_parquet(f_raw_s34)

## Match the names file against the S&P list and expand to quarters
df_names2 =expand_names(df_names,df_sp500)
df_names2.to_parquet(f_names_expanded,compression='brotli')

## Do Compustat (Fundamentals, Bus Segments, etc.)
# make sure that fundamentals data is unique permno-quarter
cusip_list=make_cusip_list(df_names)
df_fund=construct_fundamentals(pd.read_parquet(f_fundamentals),df_names2)
df_bus=construct_bus_segments(pd.read_parquet(f_segments),df_sp500)
df_fund2=pd.merge(df_fund,df_bus,on=['permno','quarter'],how='outer')
df_fund2.to_parquet(f_comp_info,compression='brotli')


# ### Merge and  Drops ~ 5m
# - Merge: Permno information from CRSP names file to 13-F filings
# - Drop: Non S&P 500 component filings from 13-f's
# - Fix: Adjust Blackrock dates because of known reporting issue (see https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/research-guides/research-note-regarding-thomson-reuters-ownership-data-issues/)
# - Merge: stock split information from MSF file (cfacshr) (https://wrds-support.wharton.upenn.edu/hc/en-us/articles/115003101112-Adjusting-Splits-Using-CRSP-Data)
# - Fix: Select a single Filing Date (Fdate) for each Rdate.
#     - 24,432,318 Obs have single observation
#     -  2,608,149 Obs have multiple filings with same shares (different prices)
#     - 84,159 Obs have a known share split: take the first filing (before share split)
#     - 44,874 Obs have no known share split: take the last filing (assume these are corrections)
# - Merge and Consolidate: Managers using consolidation file (Blackrock, Inc --> Blackrock, etc.)
# - Calculate $\beta_{fs}$ for each quarter in LONG format.
# - Add possible drops:  by permno (dual class shares, ADR's,etc.), share class (ADR's, REITs,etc.)


## Process Thomson-Reuters $\beta$
# this needs about 20 GB of RAM
# 1. Apply fixes and merges described above
s34_data=filter_sp(add_permno(blackrock_fix(raw_s34),cusip_list),df_sp500)
main_df=consolidate_mgrs(dedup_s34(add_stock_splits(s34_data,df_msf2)),f_mgr_consolidations)
df1=compute_betas(main_df,df_msf2)
df1=add_drops(df1,f_permno_drops,df_names2)
df1.to_parquet(f_betas_unfiltered,compression='brotli')

### Process Scraped 13F's ~3min
# 1. Append it to the existing dataset
# 2. Add the drops

dfs=process_scraped(f_scrape,f_big4)
dfs=add_drops(dfs,f_permno_drops,df_names2)
dfs.to_parquet(f_betas_scraped,compression='brotli')

### Combine Both Sets of $\beta$s
# - Use TR data before 2001
# - Use scraped data after 2001
# - Save the combined FrankenBeta file

# use TR before cut-date and scraped data after
df=combine_betas(df1,dfs,cut_dat e='2000-01-01')
df.to_parquet(f_frankenbetas,compression='brotli')

### Checks
# 1. Tabulate: Missing Shares Outstanding (TR), Missing Price Info (TR), Duplicate Observations within an Fdate/Rdate and Permno, Manager
# 2. Tabulate: 18 cases where firm exist in S&P500 but not in names file (yet).
# 3. 1057 Observations (Firm-Quarter) in S&P500 but not in S34 Data (959 after 2010).
# 4. 924 Observations with multiple CUSIPS in same period for same firm (these are filings with typos, weird share classes, etc.)

print(checks_dir)
## Define the Checks 
f_notin_crsp = checks_dir / 'compustat-notin-crsp.xlsx'
f_shares_out = checks_dir / 's34-no-shares.xlsx'
f_prc_zero = checks_dir / 's34-zero-price.xlsx'
f_duplicates = checks_dir / 's34_duplicate_permno.xlsx'
f_names_missing=checks_dir / 'unmatched-names-splist.xlsx'
f_s34_coverage = checks_dir / 'coverage_s34.xlsx'
f_multiple_cusips = checks_dir / 'multiple_cusips.xlsx'
f_multiple_cusips_summary = checks_dir / 'multiple_cusips_summary.xlsx'
f_missing_betas = checks_dir / 'missing_betas.xlsx'
f_missing_atq = checks_dir / 'missing_atq.xlsx'
f_missing_segments = checks_dir / 'missing_segments.xlsx'
f_bigbeta_1 =  checks_dir / 'big_betas_tr.xlsx'
f_bigbeta_2 =  checks_dir / 'big_betas_scrape.xlsx'


## Run the Checks
check_s34(s34_data, f_shares_out, f_prc_zero, f_duplicates) 
check_names(df_sp500,df_names, f_names_missing)
check_s34_coverage(df1,df_sp500,df_names , f_s34_coverage)
check_multiple_cusip(s34_data, f_multiple_cusips, f_multiple_cusips_summary)
check_bigbeta(df1,f_bigbeta_1)
check_bigbeta(dfs,f_bigbeta_2)
check_fundamental_coverage(df,df_fund2,df_names2, f_missing_betas, f_missing_atq, f_missing_segments)
