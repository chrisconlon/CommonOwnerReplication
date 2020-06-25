# Step 1: Download Data from WRDS
# Note: you will need a WRDS account for wrds.Connection() to work
import pandas as pd
import wrds
from our_plot_config import wrds_dir
from wrds_downloads import clean_wrds, get_names, get_crosswalk 
from wrds_downloads import  get_fundamentals, get_short_interest

from wrds_downloads import  get_segments, get_msf, get_s34
# raw data pulls -- save in "WRDS" directory
f_raw_s34= wrds_dir / 'raw_s34.parquet'
f_splist = wrds_dir /'sp500_list.parquet'
f_crsp_names = wrds_dir / 'crsp_names.parquet'
f_msf_data = wrds_dir / 'crsp_msf.parquet'
f_short = wrds_dir / 'short_interest.parquet'
f_fundamentals= wrds_dir / 'fundamentals_data.parquet'
f_segments= wrds_dir / 'wrds_segments.parquet'
f_managers= wrds_dir / 'manager_list.parquet'
f_managers_all= wrds_dir / 'manager_list_all.parquet'


### Pull the Data from WRDS ~20 min (ENTIRE FILE)
## This file requires about 48GB of RAM available

db = wrds.Connection()

### Pull the ID/Crosswalk Tables
# - Pull the S&P 500 Constituents List (CRSP)
# - Pull the "names" file: this maps permno to CUSIP, and NCUSIP (current period), and SIC code by date
# - Pull the Compustat link file : Construct a unique mapping from gvkey (Compustat) to Permno (CRSP)
# - Save the raw (un-filtered by time or S&P membership) files

## This block is < 1m
df_sp500= clean_wrds(db.get_table('crsp','DSP500LIST'))
df_sp500.to_parquet(f_splist)
print("First File Done: WRDS connection is probably ok")


# Filter S&P List: Ignore pre-1980 components
df_sp500=df_sp500[df_sp500.ending> '1979-12-31']

df_names=get_names(db)
df_names.to_parquet(f_crsp_names)

# Grab all possible CUSIPS by Permno
df_names2=pd.merge(df_sp500,df_names,on='permno')
df_names2=df_names2[~((df_names2['ending'] < df_names2['st_date'])|(df_names2['start'] > df_names2['end_date']))]

# Get unique list of CUSIPs and Permno's for SQL queries
all_cusips=list(set(df_names2.cusip).union(df_names2.ncusip))
all_permnos = list(df_names2.permno.unique().astype(int))

crosswalk= get_crosswalk(db,all_permnos)

### Pull the CRSP and Compustat Data Files (< 1m)
# Pull the Compustat Short Interest File 
# - Add permno's to short interest table
# - Convert Short interest table to quarterly observations
# - Take last observations within each Permno, Quarter
# 
# Pull the Compustat Fundamentals Data
# - Add permnos and CUSIPS to the Fundamentals data
# 
# Pull the Compustat Business Segments Data
# - Just count the number of segments
# - Add permnos to number of segments
# 
# Pull the Crisp Price and Shares Oustanding MSF Data
# - Save to parquet (around 2MB compressed)
# - Use this to get a single price, shares_oustanding for each security quarter

df_fund=get_fundamentals(db,crosswalk)
df_fund.to_parquet(f_fundamentals)

df_short = get_short_interest(db,crosswalk)
df_short.to_parquet(f_short,compression='brotli')

df_fund=get_fundamentals(db,crosswalk)
df_fund.to_parquet(f_fundamentals)

df_seg=get_segments(db,crosswalk)
df_seg.to_parquet(f_segments,compression='brotli')

df_msf2 = get_msf(db,all_permnos,False)
df_msf2.to_parquet(f_msf_data,compression='brotli')


# Get Managers and stock names
df_m=db.get_table('tfn','s34type1')
df_m.to_parquet(f_managers_all,compression='brotli')

names=db.get_table('crsp','stocknames')
names.to_parquet(wrds_dir / 'all_names.parquet')

# #### Pull the S-34 Data -- This is SLOW don't re-run ~15m
# - Only get for 8-digit CUSIPs in our S&P dataset
# - This is VERY slow and around 5.5 GB (320MB on disk)
# - Use this to get holdings for each 13-F investor (Don't trust self reported prices or shares outstanding)

print("Starting s34 Download...")
s34_data = get_s34(db,all_cusips)
s34_data.to_parquet(f_raw_s34,compression='brotli')
print("S34 Complete!")


# unique list of manager names
mgr_list=s34_data.groupby(['mgrno'])['mgrname'].agg(pd.Series.mode).reset_index()
mgr_list['mgrname']=mgr_list['mgrname'].astype(str)
mgr_list.to_parquet(f_managers,compression='brotli')