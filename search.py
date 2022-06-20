import warnings
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import fetch_data as f
import multi_thread as m
from IPython.display import display
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

# 1. type in date/film_type -> return three separate csv files
# 2. clean the data
# 3. manually check integrity
# 4. merge with the main csv files(rank, fp_info, tmdb, main), including necessary re-calculation (word cloud/ clusters)

# crawler headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'}


class InitSearch:
    """
    This class mainly serve two purpose
    1. Store all the scraped and fetched data(Rank Info, FlixPatrol Info and TMDb Info) for spectating.
    2. Automate some routine process(getting dates, areas, produce url for fetching rank info and calculate the error data)
    """

    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
        self.areas = self.get_area()
        self.dates = self.get_date()
        self.url = ['https://flixpatrol.com/top10/streaming/{}/{}'.format(area, date)
                    for area in self.areas for date in self.dates]
        self.rank = pd.DataFrame(
            columns=["platform", "film_type", "date", "country", "film_rank", "film_title", "film_par"])
        self.flixpatrol_info = pd.DataFrame(columns=["film_type", "film_par", "film_title", "film_country", "film_date",
                                                     "film_genre", "film_tag", "series", "film_starring",
                                                     "film_director",
                                                     "imdb", "rottentomatoes", "summary"])
        self.tmdb_info = pd.DataFrame(
            columns=["film_type", 'film_par', 'id', 'zh_title', 'release_date', 'film_country',
                     'film_countries_list', 'directors', 'casts', 'production_companies',
                     'collection', 'zh_overview', 'en_overview', 'genre_list', "動作", "冒險",
                     "動畫", "喜劇", "犯罪", "紀錄", "劇情", "家庭", "奇幻", "歷史", "恐怖", "音樂",
                     "解謎", "愛情", "科幻", "影劇", "驚悚", "戰爭", "西方", "動作冒險", "兒童", "新聞",
                     "科幻與奇幻", "實境秀", "脫口秀", "肥皂劇", "戰爭與政治", 'keyword_list'])
        self.rank_error = pd.DataFrame(columns=['country', 'date', 'url'])
        self.flixpatrol_error = pd.DataFrame(columns=['film_type', 'film_par'])
        self.tmdb_error = pd.DataFrame(columns=["film_type", 'film_par', 'film_title', 'film_date'])

    def get_date(self):
        date_list = []
        start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
        while start_date <= end_date:
            date_str = start_date.strftime("%Y-%m-%d")
            date_list.append(date_str)
            start_date += datetime.timedelta(days=1)
        return date_list

    def get_area(self):
        # areas
        url = "https://flixpatrol.com/top10/"
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        areas = ['world']
        area_label = soup.findAll('div', attrs={'x-data': "App.UI.Dropdown()"})[1].findAll('a',
                                                                                           attrs={'role': "menuitem"})
        for area in area_label:
            areas.append(area["href"][17:-12])
        return areas

    def export_data(self, df: pd.DataFrame, file_name: str):
        df.to_csv('{}_{}~{}.csv'.format(file_name, self.start_date, self.end_date), index=False, encoding='utf-8-sig')

    def data_error(self):
        # rank_error
        all_areadates = set(itertools.product(self.areas, self.dates))
        fetched_areas = self.rank['country'].sort_values().unique().tolist()
        fetched_dates = self.rank['date'].unique().tolist()
        fetched_areadates = set(itertools.product(fetched_areas, fetched_dates))
        missed_rank_data = all_areadates - fetched_areadates
        rank_errors = [{'country': error[0], 'date': error[1],
                        'url': 'https://flixpatrol.com/top10/streaming/{}/{}'.format(error[0], error[1])}
                       for error in missed_rank_data]
        self.rank_error = self.rank_error.append(rank_errors, ignore_index=True)
        self.rank_error = self.rank_error.sort_values(['country', 'date'])
        # flixpatrol_error
        self.flixpatrol_error = self.flixpatrol_info[self.flixpatrol_info['film_title'].isna()][
            ['film_par']].drop_duplicates()
        # tmdb_error
        missed_film_pars = set(self.flixpatrol_info[~self.flixpatrol_info['film_title'].isna()]['film_par'].tolist()) \
                           - set(self.tmdb_info['film_par'].tolist())
        self.tmdb_error = self.flixpatrol_info[self.flixpatrol_info['film_par'].isin(missed_film_pars)][
            ['film_type', 'film_par', 'film_title', 'film_date']].drop_duplicates()
        error_freq = {}
        for film_par in missed_film_pars:
            error_freq[film_par] = self.rank[self.rank['film_par'] == film_par].shape[0]
        self.tmdb_error['frequency'] = self.tmdb_error['film_par'].apply(lambda x: error_freq[x])
        self.tmdb_error.sort_values(['frequency'], ascending=False, inplace=True)


if __name__ == '__main__':
    y = InitSearch('2022-01-01', '2022-03-10')
    y.rank = f.pipeline_1(y.url)
    y_2021.flixpatrol_info = pd.read_csv("FlixPatrol Info_2021-07-01~2021-12-31.csv")

    fp_info_list = y_2021.flixpatrol_info.to_dict('records')
    y_2021.tmdb_info = m.multi_thread(f.pipeline_3, fp_info_list, 500)
    y_2021.export_data(y_2021.tmdb_info, 'TMDb Info')

    y_2021.data_error()
    y_2021.export_data(y_2021.rank_error, 'Rank Error')
    y_2021.export_data(y_2021.flixpatrol_error, 'FlixPatrol Error')
    y_2021.export_data(y_2021.tmdb_error, 'TMDb Error')
