from collections import defaultdict
import numpy as np
import pandas as pd
from scipy.stats import wasserstein_distance
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score



def process_data(data, training_data_per_env=-1):
    grouped_data = defaultdict(list)

    df = data[(data['server_cpu'] > 0) & (data['client_cpu'] > 0) & (data['network'] > 0)]

    for combination, group in df.groupby(['server_cpu', 'client_cpu', 'network']):

        if training_data_per_env != -1:
            group = group.head(training_data_per_env)

        grouped_data[combination].append(group)

    grouped_data = {key: pd.concat(frames, ignore_index=True) for key, frames in grouped_data.items()}

    clusters = _cluster_groups(grouped_data, column='time')

    group_to_rows = {}
    for group_key, group_df in grouped_data.items():
        group_to_rows[group_key] = df[
            (df['server_cpu'] == group_key[0]) &
            (df['client_cpu'] == group_key[1]) &
            (df['network'] == group_key[2])
            ]

    # Split data based on clusters
    cluster_dataframes = {}
    for cluster_index, cluster in enumerate(clusters):
        cluster_key = "cluster-" + "-".join(
            f"{group_key[0]}_{group_key[1]}_{group_key[2]}" for group_key in cluster
        )

        # Combine rows for this cluster
        cluster_rows = pd.concat([group_to_rows[group_key] for group_key in cluster], ignore_index=True)
        cluster_dataframes[cluster_key] = cluster_rows

    return cluster_dataframes


def _cluster_groups(grouped_data, column):
    group_keys = list(grouped_data.keys())
    group_frames = list(grouped_data.values())

    n = len(group_keys)
    similarity_matrix = np.zeros((n, n))

    for i in range(n):
        for j in range(i + 1, n):
            distance = _calculate_similarity(group_frames[i], group_frames[j], column)
            similarity_matrix[i, j] = distance
            similarity_matrix[j, i] = distance

    optimal_clusters, scores = _find_optimal_clusters_with_silhouette(similarity_matrix, max_clusters=len(group_keys) - 1)

    actual_clusters = min(optimal_clusters*2, len(group_keys) - 1)

    clustering = AgglomerativeClustering(n_clusters=actual_clusters, affinity='precomputed', linkage='average')
    labels = clustering.fit_predict(similarity_matrix)

    clusters = defaultdict(list)
    for i, label in enumerate(labels):
        clusters[label].append(group_keys[i])

    return list(clusters.values())


def _calculate_similarity(group1, group2, column):
    values1 = group1[column].values
    values2 = group2[column].values
    return wasserstein_distance(values1, values2)


def _find_optimal_clusters_with_silhouette(similarity_matrix, max_clusters=10):
    silhouette_scores = []
    max_clusters = int(max_clusters)

    for n_clusters in range(2, max_clusters + 1):
        # Perform clustering
        clustering = AgglomerativeClustering(n_clusters=n_clusters, affinity='precomputed', linkage='average')
        labels = clustering.fit_predict(similarity_matrix)
        # Calculate silhouette score
        silhouette_avg = silhouette_score(similarity_matrix, labels, metric='precomputed')
        silhouette_scores.append(silhouette_avg)

    # Take clustering with best score
    optimal_clusters = np.argmax(silhouette_scores) + 2  # Add 2 because range starts at 2
    return optimal_clusters, silhouette_scores


def _group_data_by_environment(file_list):
    """
    Groups data by unique combinations of 'server_cpu', 'client_cpu', and 'network'.

    Parameters:
        file_list (list): List of file paths to load.

    Returns:
        dict: Grouped data with keys as combinations and values as concatenated DataFrames.
    """
    grouped_data = defaultdict(list)

    for file_path in file_list:
        df = pd.read_csv(file_path[0])
        df = df[(df['server_cpu'] > 0) & (df['client_cpu'] > 0) & (df['network'] > 0)]

        for combination, group in df.groupby(['server_cpu', 'client_cpu', 'network']):
            grouped_data[combination].append(group)

    return {key: pd.concat(frames, ignore_index=True) for key, frames in grouped_data.items()}