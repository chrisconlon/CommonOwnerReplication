import numpy as np
import pandas as pd
from utilities.date_util import lookup_dates, end_quarter

# Download the CRSP Names file
# - this links the permno to gvkey (COMPUSTAT) and CUSIP
# - Fix the date ending only for last date within the group


def get_names(db):
    return fix_ending_dates(clean_wrds(db.get_table(
        'crsp', 'stocknames')), 'nameenddt', ['permno'])

# Get the Compustat-CRSP Link table
# - fix the dates and use todays date as empty end_date
# - filter on permnos


def get_crosswalk(db, permno_list):
    crosswalk = clean_wrds(db.get_table('crsp', 'Ccmxpf_linktable'))
    crosswalk = clean_wrds(crosswalk[~(
        crosswalk.linkenddt < '1980-01-01') & crosswalk.lpermno.isin(permno_list)])
    crosswalk['linkenddt'].fillna(pd.Timestamp("today").date(), inplace=True)
    return crosswalk

# DB Queries

# Get the Compustat Fundamentals
# Match to names file to get permno instead of gvkey
# Make sure they are unique observations by permno,quarter (this is a pain)


def get_fundamentals(db, crosswalk):
    fields = [
        'gvkey',
        'datadate',
        'fyearq',
        'fqtr',
        'fyr',
        'datafqtr',
        'indfmt',
        'cusip',
        'oibdpq',
        'atq',
        'niq',
        'saleq',
        'cogsq']
    query = "select " + \
        ', '.join(fields) + " from comp.fundq where fyearq> 1979 and gvkey in %s" % repr(
            tuple(crosswalk.gvkey.unique()))
    df_fundq = clean_wrds(db.raw_sql(query)).sort_values(['gvkey', 'datafqtr'])
    # remove duplicates by taking last datafqtr within each gvkey-quarter
    # note: this is rare and only happens when fiscal year changes, taking
    # first has no effect
    df_fundq2 = df_fundq.groupby(['gvkey', 'datadate']).last().reset_index()

    # merge in the gvkey-permno crosswalk
    x = pd.merge(df_fundq2,
                 crosswalk[['gvkey',
                            'lpermno',
                            'linkdt',
                            'linkenddt']].drop_duplicates(),
                 on='gvkey').rename(columns={'lpermno': 'permno'})
    y = x[(x.datadate >= x.linkdt) & (x.datadate <= x.linkenddt)].copy()
    return clean_wrds(y.sort_values('linkenddt').groupby(
        ['permno', 'datadate']).last().reset_index()[fields + ['permno']])

# Download the MSF file from CRSP
# - convert to quarterly data by taking last observation


def get_msf(db, permno_list, trim=False):
    fields = [
        'cusip',
        'permno',
        'hsiccd',
        'date',
        'prc',
        'altprc',
        'shrout',
        'altprcdt',
        'cfacshr']
    query = "select " + \
        ', '.join(
            fields) + " from crsp.msf where date > '1979-12-31' and permno in %s" % repr(tuple(permno_list))
    df_msf = clean_wrds(db.raw_sql(query))
    df_msf2 = convert_to_quarter(df_msf, 'date', ['cusip', 'permno'])
    if trim:
        # Trim the MSF data for only dates and permnos in the S&P at the time
        df_msf3 = pd.merge(df_msf2, df_sp500, on='permno')
        return df_msf3[(df_msf3['date'] >= df_msf3['start']) &
                       (df_msf3['date'] <= df_msf3['ending'])]
    else:
        return df_msf2

# Download the short interest file from COMPUSTAT
# - Merge in the crosswalk to get permnos
# - Filter on time after merge to get correct crosswalk info


def get_short_interest(db, crosswalk):
    short_int = clean_wrds(db.get_table('comp', 'sec_shortint'))
    short_int2 = pd.merge(short_int, crosswalk, on=['gvkey'], how='left')
    short_int3 = short_int2[(short_int2.datadate <= short_int2.linkenddt) & (
        short_int2.datadate >= short_int2.linkdt)].copy()
    return convert_to_quarter(short_int3, 'datadate', ['lpermno'])[
        ['lpermno', 'lpermco', 'qdate', 'gvkey', 'iid', 'shortint', 'shortintadj', 'datadate', 'splitadjdate']]

# Download the S-34 Dataset


def get_s34(db, cusip_list):
    fields = [
        'fdate',
        'mgrname',
        'mgrno',
        'rdate',
        'cusip',
        'shares',
        'sole',
        'shared',
        'no',
        'stkname',
        'ticker',
        'indcode',
        'prc',
        'shrout1',
        'shrout2']
    fields_str = ', '.join(fields)
    query = "select " + fields_str + \
        " from tfn.s34 where rdate > '1979-12-31' and cusip in %s" % repr(
            tuple(map(str, cusip_list)))
    return clean_wrds(db.raw_sql(query))

# Download the business segments
#  - merge against crosswalk to get Permno's
#  - only need count of observations (number of segments)
#  - coverage is not great


def get_segments(db, crosswalk):
    fields = [
        'gvkey',
        'stype',
        'datadate',
        'naicss1',
        'naicss2',
        'naicss3',
        'sics1',
        'sics2',
        'sics3']
    query = "select " + \
        ', '.join(fields) + \
        " from comp_segments_hist.wrds_segmerged where stype ='BUSSEG'"
    df = db.raw_sql(query)
    df['datadate'] = pd.to_datetime(df['datadate'])
    df = df.groupby(['gvkey', 'datadate']).count()['stype'].reset_index()
    df['quarter'] = df['datadate'].apply(end_quarter)
    # these should be unique within the quarter
    df.groupby(['gvkey', 'quarter'])['stype'].last().reset_index()
    x = pd.merge(df,
                 crosswalk[['gvkey',
                            'lpermno',
                            'linkdt',
                            'linkenddt']],
                 on='gvkey').rename(columns={'lpermno': 'permno'})
    return clean_wrds(x[(x.datadate >= x.linkdt) & (x.quarter <= x.linkenddt)].copy())[
        ['permno', 'quarter', 'datadate', 'stype']]


# Generic cleaning function
# -adjusts dates to pandas format
# -adjusts integers to correct format
def clean_wrds(df):
    col_list = df.iloc[0:1].select_dtypes(exclude=[np.datetime64]).columns
    int_cols = ['permno', 'hsiccd', 'siccd', 'permco', 'shares', 'mgrno']
    date_cols = [
        'start',
        'ending',
        'namedt',
        'nameenddt',
        'st_date',
        'end_date',
        'date',
        'altprcdt',
        'fdate',
        'rdate',
        'linkdt',
        'linkenddt',
        'datadate',
        'splitadjdate']
    my_intcols = [x for x in col_list if x in int_cols]
    my_datecols = [x for x in col_list if x in date_cols]

    if my_intcols:
        df.loc[:, my_intcols] = df.loc[:, my_intcols].astype(int)
    if date_cols:
        df.loc[:, my_datecols] = df.loc[:, my_datecols].apply(
            lookup_dates, axis=0)
    return df


# Construct end of quarter date and take the last observation with group_id
def convert_to_quarter(df, date_name, group_ids):
    df.sort_values(group_ids + [date_name])
    df['qdate'] = df[date_name] - \
        pd.tseries.offsets.DateOffset(days=1) + pd.tseries.offsets.QuarterEnd()
    return df.groupby(group_ids + ['qdate']).last().reset_index()

# Adjusts date_field to correspond to end of quarter
# - Within a group_list round the date_field to the last date within the corresponding quarter
# (do this only for the final date within group)


def fix_ending_dates(df, date_field, group_list):
    df['final_date'] = df.groupby(group_list)[date_field].transform('last')
    df.loc[df[date_field] == df.final_date, date_field] = df.loc[df[date_field] == df.final_date,
                                                                 date_field] - pd.tseries.offsets.DateOffset(days=1) + pd.tseries.offsets.QuarterEnd()
    return df
