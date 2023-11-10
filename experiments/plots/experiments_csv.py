import pandas as pd
import matplotlib.pyplot as plt

df1 = pd.read_csv('1690886310_runs_comp.csv')
# df1 = pd.read_csv('1684236574_runs_comp.csv')
'''
df = pd.DataFrame({

    'nocomp': [11, 10, 22, 38, 82],
    'snappy (2:1)': [10, 11, 11, 20, 42],
    'lzo (1:1)': [12,13, 19, 38, 82],
    'lz4 (2:1)': [11, 11, 10, 19, 43],

}, index=[100, 50, 25, 13, 6])
'''

df = pd.DataFrame({
    'npar=1': df1[df1['npar'] == 1].sort_values(by=['deserpar'], ascending=True)['time'].reset_index(
        drop=True).to_numpy(),
    'npar=2': df1[df1['npar'] == 2].sort_values(by=['deserpar'], ascending=True)['time'].reset_index(
        drop=True).to_numpy(),
    'npar=4': df1[df1['npar'] == 4].sort_values(by=['deserpar'], ascending=True)['time'].reset_index(
        drop=True).to_numpy(),
    'npar=8': df1[df1['npar'] == 8].sort_values(by=['deserpar'], ascending=True)['time'].reset_index(
        drop=True).to_numpy(),
}, index=[1, 2, 4, 8, 16, 32])

print(df)
lines = df.plot.line(marker='x')
lines.set_ylim(bottom=0)



plt.title('CSV source, lineitem 10m rows, CPU:7')
plt.ylabel('Runtime (ms)')
plt.xlabel('Read parallelism (#threads)')
plt.tight_layout()
plt.savefig('experiments_csv_pars.pdf')
