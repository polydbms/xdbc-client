import matplotlib.pyplot as plt
import numpy as np
import sys

# Read the file using NumPy's genfromtxt function
logData = np.genfromtxt(sys.argv[1], delimiter=',', dtype=None, encoding=None)

labelsClient = logData[0][1::];
activeTimesClient = logData[-1][1::2];
waitTimesClient = logData[-1][2::2];

fig,ax = plt.subplots()

ax.bar(labelsClient, activeTimesClient, 0.8, label="Thread active")
ax.bar(labelsClient, waitTimesClient, 0.8, label="Thread waiting", bottom=activeTimesClient[1::])

plt.tight_layout()
plt.savefig('plot_xdbc_wait_times.png')
plt.show()