import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_samples, silhouette_score
import numpy as np
import fetch_data
import search


# crawler headers
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}

default_columns = ['platform', 'date', 'country', 'continent', 'film_type', 'film_par', 'film_title', 'film_rank',
                   'film_genre', 'genre_list', 'zh_title', 'film_country', 'film_country_iso', 'film_continent',
                   'film_countries_list', 'zh_overview', 'keyword_list']


def create_df_final(rank_info: pd.DataFrame, fp_info: pd.DataFrame, tmdb_info: pd.DataFrame, country: pd.DataFrame, columns: list):
    """
    :param rank_info: File "Rank Info"
    :param fp_info: File "FlixPatrol Info"
    :param tmdb_info: File "TMDb Info"
    :param country: File "Country"
    :param columns: The needed columns for final dataframe.
    :return: A dataframe that is used on the visualization in Tableau.

    **Note: If one can manage table relation inside Tableau, this merging step could be omitted. Yet the "film_country" columns still need to be cleaned, and "continent" and "film_continent" should be added additionally.
    """
    # take necessary columns from each info tables
    all_set = set(columns)
    rank_set = set(rank_info.columns.tolist()).intersection(all_set)
    all_set -= rank_set

    fp_set = set(fp_info.columns.tolist()).intersection(all_set)
    all_set -= fp_set
    fp_set.add('film_par')

    tmdb_set = set(tmdb_info.columns.tolist()).intersection(all_set)
    tmdb_set.add('film_par')

    rank_info_part = rank_info[rank_set]
    fp_info_part = fp_info[fp_set]
    tmdb_info_part = tmdb_info[tmdb_set]
    # merge two info dataframe
    film_info = fp_info_part.merge(tmdb_info_part, on='film_par', how='left').drop_duplicates()
    if 'film_country' in columns:
        # [film_country_tmdb]: convert iso code into country name
        iso = {}
        for _, row in country.iterrows():
            iso[row['iso']] = row['country']
        film_info['film_country_tmdb'] = film_info['film_country_iso'].apply(lambda x: iso[x] if x in list(iso.keys()) else np.nan)
        # merge [film_country] from two files
        film_info['film_country'] = film_info.apply(lambda x: x['film_country_tmdb'] if x['film_country'] is None else x['film_country'], axis=1)
        # drop columns
        film_info.drop(columns=['film_country_iso', 'film_country_tmdb'], inplace=True)
    # create column [film_continent]
    continent_dict = {}
    for _, row in country.iterrows():
        continent_dict[row[0].upper().replace(' ', '')] = row[2]
    if 'film_continent' in columns:
        film_info['film_continent'] = film_info[~film_info['film_country'].isna()]['film_country'].apply(lambda x: continent_dict[x.replace('-', '').replace(' ', '').upper()])
    # clean [zh_title]: fill na values of [zh_title] with [film_title]
    film_info['zh_title'] = film_info.apply(lambda x: x['film_title'] if x['zh_title'] == np.nan else x['zh_title'], axis=1)
    # merge with rank
    df_final = rank_info_part.merge(film_info, on='film_par', how='left').drop_duplicates()
    # create column [continent]
    if 'continent' in columns:
        df_final['continent'] = df_final['country'].apply(lambda x: continent_dict[x.replace('-', '').replace(' ', '').upper()] if x != 'world' else x)
    return df_final


def clustering(df_target):
    # df_target = df_final[(df_final['date'].str.contains(year)) & (df_final['film_type'] == film_type)]
    df_preprocess = df_target.pivot_table(index='country', columns='film_par', values='weighted_score',
                                          aggfunc='sum', fill_value=0).reset_index()
    x = df_preprocess.drop(columns=['country'])
    for n in range(4, 16):
        labels = KMeans(n_clusters=n, random_state=42).fit_predict(x.values)
        # model_name and avg_silhouette score
        avg_silhouette = silhouette_score(x, labels)
        # number of samples that are mis-classified
        sample_silhouette_score = silhouette_samples(x, labels)
        n_negative_sample_silhouette_score = sum(sample_silhouette_score < 0)
        print('For {} clusters, Avg Silhouette score: {}, Number of Negative Silhouette score samples: {}'.format(n, avg_silhouette, n_negative_sample_silhouette_score))
    n_clusters = input('Choose the number of clusters:')
    kmeans = KMeans(n_clusters=int(n_clusters), random_state=42).fit_predict(x.values)
    stacked_results = np.stack((df_preprocess['country'], kmeans), axis=1)
    label_dict = {c[0]: c[1] for c in stacked_results}
    df_target['cluster_label'] = df_target['country'].apply(lambda x: label_dict[x])
    return df_target


def correct_fp_info_error(date_range: tuple, **kwargs):
    y = search.InitSearch(date_range[0], date_range[1])
    y.rank = kwargs.get('rank', None)
    y.flixpatrol_info = kwargs.get('fp_info', None)
    y.tmdb_info = kwargs.get('tmdb_info', None)
    y.data_error()
    # fp_error and film_par_corrected
    y.flixpatrol_error['film_par_corrected'] = y.flixpatrol_error['film_par'].apply(lambda x: x.replace('?', ''))
    film_par_dict = {}
    for _, row in y.flixpatrol_error.iterrows():
        film_par_dict[row['film_par_corrected']] = row['film_par']
    # fp_info re-fetch
    newly_fetched = fetch_data.pipeline_2(y.flixpatrol_error['film_par_corrected'].values.tolist())
    # restore the false film_par of all the films that are not re-fetched
    newly_fetched['film_par'] = newly_fetched.apply(
        lambda x: film_par_dict[x['film_par']] if x['film_title'] is np.nan else x['film_par'], axis=1)
    # drop the null rows in fp_info
    y.flixpatrol_info.dropna(subset=['film_title'], inplace=True)
    # append newly fetched data into fp_info(some null rows are still in existence)
    y.flixpatrol_info = y.flixpatrol_info.append(newly_fetched, ignore_index=True)
    # export updated version of fp_info and fp_error
    y.export_data(y.flixpatrol_info, 'FlixPatrol Info')
    # tmdb_info re-fetch
    tmdb_newly_fetched = fetch_data.pipeline_3(newly_fetched[~newly_fetched['film_title'].isna()].to_dict('records'))
    y.tmdb_info = y.tmdb_info.append(tmdb_newly_fetched, ignore_index=True)
    # export updated version of tmdb_info
    y.export_data(y.tmdb_info, 'TMDb Info')
    # correct rank info "film_par" and "film_title"
    new_film_par = newly_fetched['film_par'].values.tolist()
    # change the film_par to the corrected version of film_par if the films are successfully re-fetched
    y.rank['film_par'] = y.rank.apply(
        lambda x: x['film_par'].replace('?', '') if x['film_par'].replace('?', '') in new_film_par else x['film_par'],
        axis=1)
    # some film_title in rank has "?" in them. Here only the newly fetched films are cleaned, others stay the same
    y.rank['film_title'] = y.rank.apply(
        lambda x: x['film_title'].replace('?', '') if x['film_par'].replace('?', '') in new_film_par else x[
            'film_title'], axis=1)
    # export updated version of rank
    y.export_data(y.rank, 'Rank Info')
    # update and export fp_error and tmdb_error files
    y.data_error()
    y.export_data(y.flixpatrol_error, 'FlixPatrol Error')
    y.export_data(y.tmdb_error, 'TMDb Error')


def correct_tmdb_info_error(tmdb_info, tmdb_error):
    tmdb_to_correct = tmdb_error[~tmdb_error['id'].isna()]
    for _, row in tmdb_to_correct.iterrows():
        if row['film_type'] == 'mv':
            tmdb_info = tmdb_info.append(fetch_data.tmdb_fetch_mv({'film_type': 'mv', 'film_par': row['film_par'], 'id': row['id']}), ignore_index=True)
        else:
            tmdb_info = tmdb_info.append(fetch_data.tmdb_fetch_tv({'film_type': 'tv', 'film_par': row['film_par'], 'id': row['id']}), ignore_index=True)
    return tmdb_info


if __name__ == '__main__':
    # import files
    rank = pd.read_csv('Rank Info_2020-04-01~2021-12-31.csv')
    fp = pd.read_csv('FlixPatrol Info_2020-04-01~2021-12-31.csv')
    tmdb = pd.read_csv('TMDb Info_2020-04-01~2021-12-31.csv')
    country = pd.read_excel('country.xlsx')
    extracted = pd.read_csv('Extracted.csv')
    ext_keyword = {}
    for _, row in extracted.iterrows():
        ext_keyword[row['film_par']] = row['keyword_list']
    tmdb['keyword_list'] = tmdb.apply(
        lambda x: ext_keyword[x['film_par']] if x['film_par'] in ext_keyword.keys() else x['keyword_list'], axis=1)
    cols = ['platform', 'date', 'country', 'continent', 'film_type', 'film_par', 'film_title', 'film_rank', 'film_genre',
            'genre_list', 'zh_title', 'film_country', 'film_country_iso', 'film_countries_list', 'zh_overview',
            'keyword_list', 'film_continent']
    rank = rank[rank['platform'] == 'Netflix']
    data = create_df_final(rank, fp, tmdb, country, cols)
    data.to_csv('Netflix Data.csv')








