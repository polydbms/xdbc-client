import matplotlib.pyplot as plt
import numpy as np
import sys

# Read the file using NumPy's genfromtxt function
log_data = np.genfromtxt(sys.argv[1], delimiter=', ', dtype=None, encoding=None)

start = float(log_data[0][2].rstrip('s'))
data = {}

# create dict with form: {id: (action,duration,start)}
for item in log_data:
    key = item[0]
    if key in data:
        data[key] = (item[1],float(item[2].rstrip('s')) - data[key][1], data[key][1]-start)
    else:
        data[key] = (item[1],float(item[2].rstrip('s')))

# sort for starting time
data_presorted = np.array([[key, data[key][0], data[key][1], data[key][2]] for key in data])
sorting_keys = np.argsort(data_presorted[:,3])[::-1]
data_sorted = data_presorted[sorting_keys]


# create different arrays for the bars
ids = data_sorted[:,0].astype(int)
actions = data_sorted[:,1].astype(str)
spaces = np.full_like(actions, ' ')
spaces = np.core.defchararray.add(' ',actions)
labels = np.core.defchararray.add(data_sorted[:,0],spaces)
durations = data_sorted[:,2].astype(float)
starts = data_sorted[:,3].astype(float)

# assign colors to the different actions depending on the action description of the thread.
unique_actions = np.unique(actions)
len_uniques = len(unique_actions)
cmap = plt.get_cmap('tab10')(np.arange(len_uniques))
action_to_color = dict(zip(unique_actions,cmap))
colors = np.array([action_to_color[i] for i in actions])

# build plot
fig,ax = plt.subplots()
ax.barh(labels, width=durations, left=starts, color=colors)
plt.xlabel("Time in Seconds")
plt.ylabel("Worker Threads with Id and Task")
plt.title("Duration of Different Tasks in XDBC")

# Remove axe spines
for s in ['top', 'right']:
    ax.spines[s].set_visible(False)

# duration right next to the bars
for i,bar in enumerate(ax.patches):
    plt.text(bar.get_x()+bar.get_width()+0.2,bar.get_y()+0.5*bar.get_height(), str(round((bar.get_width()), 4))+"s", fontsize=10)

plt.tight_layout()
plt.savefig('plot_xdbcclient.png')
plt.show()