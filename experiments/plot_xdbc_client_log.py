# work in progress
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib.dates import date2num
import numpy as np

# Sample log data
log_data = [
    (7019, "Client", "1699634068.544181868"),
    # ... (other log entries)
    (7019, "Client", "1699634075.055439465"),
    (7020, "Receive", "1699634072.055439465"),
    (7020, "Receive", "1699634077.055439465"),
]

start = np.array([1,   3, 6  ])
end  = np.array([0.3, 1, 0.7])
y      = np.array(["7019",   "7020",  "7030"])

plt.figure()
plt.barh(y, width=end-start, left=start)
plt.show()