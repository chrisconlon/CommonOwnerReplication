# For files and paths
import pathlib
import os


# File Directories
## cc modified to parent
proj_dir = pathlib.Path.cwd().parent
data_dir = proj_dir / 'data' 
raw_dir = data_dir / 'public'
wrds_dir = data_dir / 'wrds' 
checks_dir = data_dir / 'checks'
derived_dir =  data_dir / 'derived' 

fig_dir = proj_dir / 'figures'
tab_dir = proj_dir / 'tables'


# For plotting
#import matplotlib
#import matplotlib.pyplot as plt
#from cycler import cycler
#import seaborn as sns

# Plot Configuration
def setplotstyle():
	from cycler import cycler
	import seaborn as sns
	import matplotlib
	import matplotlib.pyplot as plt
	matplotlib.style.use('seaborn-whitegrid')

	matplotlib.rcParams.update({'font.size': 24})
	plt.rc('font', size=24)          # controls default text sizes
	plt.rc('axes', titlesize=24)     # fontsize of the axes title
	plt.rc('axes', labelsize=24)    # fontsize of the x and y labels
	plt.rc('xtick', labelsize=24)    # fontsize of the tick labels
	plt.rc('ytick', labelsize=24)    # fontsize of the tick labels
	plt.rc('legend', fontsize=24)    # legend fontsize
	plt.rc('figure', titlesize=24)
	plt.rc('axes',prop_cycle=cycler(color=['#252525', '#636363', '#969696', '#bdbdbd'])*cycler(linestyle=['-',':','--', '-.']))
	plt.rc('lines', linewidth=3)


