import numpy as np
import pandas as pd

def matlab_sparse(i,j,s,compress=True):
    rows, row_pos = np.unique(i, return_inverse=True)
    cols, col_pos = np.unique(j, return_inverse=True)
    pivoted_arr = np.zeros((len(rows), len(cols)))
    pivoted_arr[row_pos, col_pos] = s
    if compress:
    	nz=(pivoted_arr.max(axis=1)>0)
    	pivoted_arr=pivoted_arr[nz,:]
    	rows=rows[nz]
    return pivoted_arr, rows, cols

def coalesce(df,col_list,prefix,method='left'):
	for x in col_list:
		if method=='left':
			df['merged_'+x]=df[prefix+x].combine_first(df[x])
		if method=='right':
			df['merged_'+x]=df[x].combine_first(df[prefix+x])
	return df