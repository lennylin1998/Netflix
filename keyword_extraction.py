import numpy as np
from keybert import KeyBERT
import pandas as pd
from yake import KeywordExtractor
import summa
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
import multi_thread as m
import clean_data as clean


def keywords_bert(overview: str):
    """Input summary/overview in string format, and output the keywords list"""
    model = KeyBERT(model='all-mpnet-base-v2')
    keywords = model.extract_keywords(overview, keyphrase_ngram_range=(1, 1), top_n=7, use_mmr=True, diversity=0.5)
    keyword_list = [keyword[0] for keyword in keywords]
    return ','.join(keyword_list)


def keywords_yake(overview: str):
    """
    :param overview: overview string to extract keywords
    :return: keywords list(in string format, comma separated)
    """
    kw_extractor = KeywordExtractor(top=7)
    keywords = kw_extractor.extract_keywords(overview)
    keyword_list = [keyword[0] for keyword in keywords]
    return ','.join(keyword_list)


def keywords_textrank(overview: str):
    try:
        keywords = summa.keywords.keywords(overview, words=5)
    except IndexError:
        keywords = ''
    return keywords.replace('\n', ',')


def batch_keywords(df: pd.DataFrame):
    tqdm.pandas()
    df['KeyBERT'] = df['paragraph'].progress_apply(lambda x: keywords_bert(x))
    df['Yake'] = df['paragraph'].progress_apply(lambda x: keywords_yake(x))
    df['TextRank'] = df['paragraph'].progress_apply(lambda x: keywords_textrank(x))
    return df


def keywords_tfidf(corpus: list):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(corpus)
    keywords = vectorizer.get_feature_names()
    tfidf = pd.DataFrame(X.toarray(), columns=keywords)
    return_list = []
    for _, row in tfidf.iterrows():
        row.sort_values(ascending=False, inplace=True)
        return_list.append(','.join(list(row.index[:7])))
    return np.array(return_list)


def final_keywords(all_kw: set, *args):
    """
    :param all_kw: The set of all the unique keywords in the existing data.
    :param args: The string of keyword_list extracted with each keyword extraction algorithm.
    :return: The string of final version of keyword_list of that film.
    """
    kw_candidates = str(args[0]).split(',')
    for arg in args[1:]:
        kw_candidates += str(arg).split(',')
    return ','.join(list(set(kw_candidates).intersection(all_kw)))


def keywords_pipeline(tmdb_info, fp_info):
    """
    :param tmdb_info: "TMDb Info" file.
    :param fp_info: "FlixPatrol Info" file.
    :return: A dataframe of films and their extracted keyword_list.
    """
    # all_keywords
    all_keywords = []
    for keyword_list in tmdb_info['keyword_list'].values.tolist():
        all_keywords = all_keywords + str(keyword_list).split(',')
    all_keywords = set(all_keywords)
    # create to_extract
    tmdb_part = tmdb_info[['film_par', 'zh_title', 'en_overview', 'keyword_list']]
    fp_info_part = fp_info[['film_par', 'film_title', 'summary']]
    to_extract = tmdb_part.merge(fp_info_part, on='film_par', copy=False).drop_duplicates()
    # drop rows with keyword_list
    to_extract = to_extract[to_extract['keyword_list'].isna()]
    # delete the value of "summary" which starts with 'STARRING'
    to_extract['summary'] = to_extract['summary'].apply(lambda x: np.nan if 'STARRING' in str(x) else x)
    # drop rows if 'en_overview' and 'summary' both are null
    to_extract = to_extract[(~to_extract['en_overview'].isna()) | (~to_extract['summary'].isna())]
    # create column 'paragraph' which contains string of both 'en_overview' and 'summary'
    to_extract['paragraph'] = to_extract.apply(lambda x: str(x['en_overview']) + ' ' + str(x['summary']), axis=1)
    # ask for input batch size
    size = int(input("The total films to be extracted are {}.\n"
                     "It is recommended that you spilt the data into batches and submit it to multi-thread.\n"
                     "How many rows in a batch would you like?\n".format(to_extract.shape[0])))
    # multi-thread
    result = m.multi_thread(batch_keywords, to_extract, size)
    # TFIDF
    corpus = result['paragraph'].values.tolist()
    tfidf_result = keywords_tfidf(corpus)
    result['tfidf'] = tfidf_result
    # Final keywords list
    result['extracted_keyword'] = result.apply(lambda x: final_keywords(all_keywords, x['KeyBERT'], x['TextRank'], x['Yake'], x['tfidf']), axis=1)
    return result


if __name__ == "__main__":
    # import files
    rank = pd.read_csv('Rank Info_2020-04-01~2021-12-31.csv')
    fp = pd.read_csv('FlixPatrol Info_2020-04-01~2021-12-31.csv')
    tmdb = pd.read_csv('TMDb Info_2020-04-01~2021-12-31.csv')
















