import pandas as pd

# Read the CSV file into a DataFrame
df = pd.read_csv('xdbc_experiments_master.csv')

# Columns to consider when checking for duplicates
cols_to_check = [
    "system", "table", "compression", "format", "network_parallelism",
    "bufpool_size", "buff_size", "network", "client_readmode", "client_cpu",
    "client_write_par", "client_decomp_par", "server_cpu", "server_read_par",
    "server_read_partitions", "server_deser_par", "server_comp_par"
]

# STEP 1: Extract rows where time <= 3
# Capture these rows before any other operation since we are modifying the original DataFrame later
rows_with_time_less_than_three = df[df['time'] <= 3]

# Save these rows to a separate CSV file
rows_with_time_less_than_three.to_csv('rows_with_time_less_than_three.csv', index=False)

# Print indices of rows where time <= 3 for verification
time_less_than_three_rows_indices = rows_with_time_less_than_three.index.tolist()
# print(f"Indices of rows where time <= 3: {time_less_than_three_rows_indices}")

# STEP 2: Remove rows where time <= 3 from the original DataFrame
df = df[df['time'] > 3]

# STEP 3: Identify duplicates based on specified columns
# We're working on the DataFrame that has already had 'time' <= 3 rows removed
duplicates = df[df.duplicated(cols_to_check, keep=False)].sort_values(by=cols_to_check)

# Save duplicates to a new CSV for inspection, if needed
duplicates.to_csv('duplicated_rows_sorted_for_inspection.csv', index=False)

# STEP 4: Remove duplicates from the DataFrame
df_no_duplicates = df.drop_duplicates(cols_to_check, keep='first')

# Save the clean DataFrame to a new CSV file
df_no_duplicates.to_csv('xdbc_experiments_master_without_duplicates.csv', index=False)

# Print information for verification
print(
    f"Total rows (original): {len(df) + len(duplicates) + len(rows_with_time_less_than_three)}")  # Accounting for all modifications
print(f"Total rows (after removing time <= 3 and duplicates): {len(df_no_duplicates)}")
print(f"Number of duplicate rows removed: {len(duplicates)}")
