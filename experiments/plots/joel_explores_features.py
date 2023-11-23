import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import seaborn as sns

# load data and clean it
df = pd.read_csv("current_analysis/xdbc_experiments_master.csv")
data = df.iloc[:, 5:-2] # information strings and datasize cut, because size is always the same and rest should have no influence
times = df.iloc[:, -2] # have the times
data = data[times < 10000]
times = times[times < 10000] # filter outliers
print(times.head())
compressions = np.unique(data.iloc[:, 0:1])
compression_dict = dict(zip(compressions, np.arange(compressions.shape[0])))
data['compression'] = data['compression'].apply(lambda x: compression_dict.get(x))
print("Loaded Dataset:\n", data.head(), "\n", "Dictionary used: \n", compression_dict)

# extract environment combinations
environ_params = data[['network','client_cpu','server_cpu']]
environs = np.unique(np.array(environ_params),axis=0)
print(environs)

# create correlation matrix for non-discrete values
non_disc = data[['network_parallelism','buff_size','network','client_cpu','client_read_par','client_decomp_par','server_cpu','server_read_par','server_read_partitions','server_deser_par']]
plots = []

print("Creating correlations...")
fig = plt.figure()
corr = pd.concat([non_disc, times], axis=1).corr().round(2)
sns.heatmap(corr, vmin=-1, vmax=1, center=0, cmap='vlag', annot=True)
fig.suptitle("Correlations")
plots.append(fig)

print("Creating parameter to time scatters...")
for i, column in enumerate(non_disc):
    print(column)
    fig, ax = plt.subplots()
    ax.scatter(times, non_disc[column], rasterized=True)
    ax.set_title(column)
    ax.set_xlabel("time in s")
    plots.append(fig)


plt.tight_layout()
plt.show()

# storing plots in pdf
# print("Storing plots...")
# pp = PdfPages('plots.pdf')
# for plot in plots:
#     pp.savefig(plot)
# pp.close()


