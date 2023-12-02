import matplotlib.pyplot as plt
import numpy as np
import sys

# Read the file using NumPy's genfromtxt function
logData1 = np.genfromtxt(sys.argv[1], delimiter=',', dtype=None, encoding=None)
logData2 = np.genfromtxt(sys.argv[2], delimiter=',', dtype=None, encoding=None)

fig,(ax1, ax2) = plt.subplots(2,1)
ax1.bar(logData1[0][1::2], logData1[-1][1::2].astype(float), 0.8, label="Thread active")
ax1.bar(logData1[0][3::2], logData1[-1][2::2].astype(float), 0.8, label="Thread waiting", bottom=logData1[-1][3::2].astype(float))
ax2.bar(logData2[0][1::2], logData2[-1][1::2].astype(float), 0.8, label="Thread active")
ax2.bar(logData2[0][3::2], logData2[-1][2::2].astype(float), 0.8, label="Thread waiting", bottom=logData2[-1][3::2].astype(float))

plt.tight_layout()
plt.savefig('plot_xdbc_wait_times.png')
plt.show()