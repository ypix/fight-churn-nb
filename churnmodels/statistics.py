import pandas as pd
import numpy as np

from collections import Counter
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform


def dataset_stats(churn_data):
    if 'is_churn' in churn_data:
        churn_data['is_churn'] = churn_data['is_churn'].astype(float)

    summary = churn_data.describe()
    summary = summary.transpose()
    # print(churn_data)

    summary['skew'] = churn_data.skew()
    summary['1%'] = churn_data.quantile(q=0.01)
    summary['99%'] = churn_data.quantile(q=0.99)
    summary['nonzero'] = churn_data.astype(bool).sum(axis=0) / churn_data.shape[0]

    summary = summary[['count', 'nonzero', 'mean', 'std', 'skew', 'min', '1%', '25%', '50%', '75%', '99%', 'max']]
    summary.columns = summary.columns.str.replace("%", "pct")
    return summary


def metric_scores(churn_data, stats, skew_thresh=4.0):
    data_scores = churn_data.copy()
    data_scores = data_scores.drop(['is_churn'], axis=1)

    stats = stats.drop('is_churn')
    skewed_columns = (stats['skew'] > skew_thresh) & (stats['min'] >= 0)
    skewed_columns = skewed_columns[skewed_columns]

    for col in skewed_columns.keys():
        data_scores[col] = np.log(1.0 + data_scores[col])
        stats.at[col, 'mean'] = data_scores[col].mean()
        stats.at[col, 'std'] = data_scores[col].std()

    data_scores = (data_scores - stats['mean']) / stats['std']
    data_scores['is_churn'] = churn_data['is_churn'].astype('bool')
    # data_scores["observation_date"]=pd.to_datetime(churn_data["observation_date"])
    return data_scores


def apply_metric_groups(score_data, load_mat_df):
    # data_2group = score_data.drop('is_churn',axis=1)
    data_2group = score_data
    load_mat_ndarray = load_mat_df.to_numpy()

    # Make sure the data is in the same column order as the rows of the loading matrix
    ndarray_2group = data_2group[load_mat_df.index.values].to_numpy()
    grouped_ndarray = np.matmul(ndarray_2group, load_mat_ndarray)

    churn_data_grouped = pd.DataFrame(grouped_ndarray, columns=load_mat_df.columns.values, index=score_data.index)

    churn_data_grouped['is_churn'] = score_data['is_churn']
    return churn_data_grouped


def make_load_matrix(labeled_column_df, metric_columns, relabled_count, corr):
    load_mat = np.zeros((len(metric_columns), len(relabled_count)))
    for row in labeled_column_df.iterrows():
        orig_col = metric_columns.index(row[1][1])
        if relabled_count[row[1][0]] > 1:
            load_mat[orig_col, row[1][0]] = 1.0 / (np.sqrt(corr) * float(relabled_count[row[1][0]]))
        else:
            load_mat[orig_col, row[1][0]] = 1.0

    is_group = load_mat.astype(bool).sum(axis=0) > 1
    column_names = ['metric_group_{}'.format(d + 1) if is_group[d]
                    else labeled_column_df.loc[labeled_column_df['group'] == d, 'column'].iloc[0]
                    for d in range(0, load_mat.shape[1])]
    loadmat_df = pd.DataFrame(load_mat, index=metric_columns, columns=column_names)
    loadmat_df['name'] = loadmat_df.index
    sort_cols = list(loadmat_df.columns.values)
    sort_order = [False] * loadmat_df.shape[1]
    sort_order[-1] = True
    loadmat_df = loadmat_df.sort_values(sort_cols, ascending=sort_order)
    loadmat_df = loadmat_df.drop('name', axis=1)
    return loadmat_df


def relabel_clusters(labels, metric_columns):
    cluster_count = Counter(labels)
    cluster_order = {cluster[0]: idx for idx, cluster in enumerate(cluster_count.most_common())}
    relabeled_clusters = [cluster_order[l] for l in labels]
    relabled_count = Counter(relabeled_clusters)
    labeled_column_df = pd.DataFrame({'group': relabeled_clusters, 'column': metric_columns}) \
        .sort_values(['group', 'column'], ascending=[True, True])
    return labeled_column_df, relabled_count


def find_correlation_clusters(corr, corr_thresh=0.5):
    dissimilarity = 1.0 - corr
    hierarchy = linkage(squareform(dissimilarity), method='single')
    diss_thresh = 1.0 - corr_thresh
    labels = fcluster(hierarchy, diss_thresh, criterion='distance')
    return labels


def find_metric_groups(score_data, group_corr_thresh=0.5):
    if 'is_churn' in score_data.columns:
        score_data.drop('is_churn', axis=1, inplace=True)
    metric_columns = list(score_data.columns.values)

    labels = find_correlation_clusters(score_data.corr(), group_corr_thresh)
    labeled_column_df, relabled_count = relabel_clusters(labels, metric_columns)
    loadmat_df = make_load_matrix(labeled_column_df, metric_columns, relabled_count, group_corr_thresh)
    # save_load_matrix(data_set_path,loadmat_df,labeled_column_df)
    return loadmat_df


def transform_skew_columns(data, skew_col_names):
    for col in skew_col_names:
        if col in data.columns:
            data[col] = np.log(1.0 + data[col])


def transform_fattail_columns(data, fattail_col_names):
    for col in fattail_col_names:
        if col in data.columns:
            data[col] = np.log(data[col] + np.sqrt(np.power(data[col], 2) + 1.0))


def fat_tail_scores(churn_data, stats, skew_thresh=4.0, **kwargs):
    data_scores = churn_data.copy()
    if "is_churn" in data_scores.columns:
        data_scores.drop('is_churn', inplace=True, axis=1)
    if "is_churn" in stats.columns:
        stats.drop('is_churn', inplace=True)

    skewed_columns = (stats['skew'] > skew_thresh) & (stats['min'] >= 0)
    transform_skew_columns(data_scores, skewed_columns[skewed_columns].keys())

    fattail_columns = (stats['skew'] > skew_thresh) & (stats['min'] < 0)
    transform_fattail_columns(data_scores, fattail_columns[fattail_columns].keys())

    mean_vals = data_scores.mean()
    std_vals = data_scores.std()
    data_scores = (data_scores - mean_vals) / std_vals

    if "is_churn" in churn_data.columns:
        data_scores['is_churn'] = churn_data['is_churn']

    param_df = pd.DataFrame({'skew_score': skewed_columns,
                             'fattail_score': fattail_columns,
                             'mean': mean_vals,
                             'std': std_vals})
    return param_df, data_scores
