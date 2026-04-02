import re
from collections import Counter, defaultdict, namedtuple
from functools import wraps, reduce
from datetime import datetime
from pathlib import Path
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import pytest
from random import randint, choice


def qsplit(s: str) -> list:
    """Split csv row containing double-quoted values"""
    if '"' not in s:
        return s.split(',')
    field, fields, in_quotes = [], [], False
    for c in s:
        if c == '"':
            in_quotes = not in_quotes
            field.append(c)
        elif c == ',' and not in_quotes:
            fields.append(''.join(field))
            field = []
        else:
            field.append(c)
    if not in_quotes:
        fields.append(''.join(field))
        return fields
    raise ValueError("Odd number of double quotes in string: {s}")

def qshave(s: str) -> str:
    """Shave quotes from double-quoted string"""
    return s[1:-1] if s and s[0] == s[-1] == '"' else s

def parse_csv(filename: str, col_names: list, N: int = 1000) -> list:
    """
    Read csv file up to N rows of data or till end of file.
    The CSV header must match the list of column names.
    """
    try:
        with open(filename, 'r') as fp:
            header = qsplit(fp.readline().strip())
            if header != list(col_names):
                raise ValueError(f"Headers in file {filename} should be: {', '.join(col_names)}")
            for i, line in zip(range(N), fp): # iterate until N or EOF
                row = qsplit(line.strip())
                if len(row) != len(col_names):
                    raise ValueError(f"Wrong number of columns in string {i+2} of file {filename}")
                yield row
    except OSError as e:
        raise OSError(f"Could not read file {filename}: {e}") from e
    except ValueError:
        raise
    except Exception as e:
        raise Exception(f"Could not parse csv file {filename}. {type(e).__name__}: {e}") from e

def attr_list(x) -> list:
    return list(vars(x).keys())


class Movies:
    """Analyzing data from movies.csv"""
    def __init__(self, path_to_the_file, N: int = 1000):
        self.N = N
        self.COLS = ['movieId', 'title', 'genres']
        self.movieId, self.title, self.genres, self.year = [], [], [], []
        try:
            for row in parse_csv(path_to_the_file, self.COLS, self.N):
                self.movieId.append(int(row[0]))
                self.title.append(qshave(row[1]))
                genres = row[2].split('|')
                self.genres.append(genres if genres[0] != '(no genres listed)' else [])
                year = int(match.group()[1:-1]) if (match := re.search(r"\([0-9]{4}\)", row[1])) else None
                self.year.append(year)
            self.movie_index = {Id: i for i, Id in enumerate(self.movieId)}
        except ValueError as e:
            print(f"Wrong data format in row {len(self.movieId)}: {e}")
        except Exception as e:
            print(f"Processing data row {len(self.movieId)} failed. {type(e).__name__}: {e}")

    def dist_by_release(self):
        """The method returns a dict where the keys are years and the values are counts."""
        release_years = Counter(self.year).most_common()
        return dict(release_years)
    
    def dist_by_genres(self):
        """The method returns a dict where the keys are genres and the values are counts."""
        genres_count = Counter(genre for genres in self.genres for genre in genres).most_common()
        return dict(genres_count)
    
    def most_genres(self, n):
        """
        The method returns a dict with top-n movies where the keys are movie titles and 
        the values are the number of genres of the movie.
        """
        movies = list(zip(self.title, map(len, self.genres)))
        movies_dict = dict(sorted(movies, key=lambda x: x[1]))
        movies_items = list(movies_dict.items())[:-1-n:-1]
        return dict(movies_items)
    
    def title_search(self, words: list, n: int = 100) -> list:
        """Return list of movie titles which contain all listed words"""
        result = []
        if words:
            for i, title in enumerate(self.title):
                title_lower = title.lower()
                if all(word and (word.lower() in title_lower) for word in words):
                    result.append((self.movieId[i], self.title[i]))
        return dict(result[:n])


class Ratings:
    """
    Analyzing data from ratings.csv
    """
    def __init__(self, path_to_the_file, N: int = 100000):
        self.N = N
        self.COLS = ['userId', 'movieId', 'rating', 'timestamp']
        self.userId, self.movieId, self.rating, self.year = [], [], [], []
        try:
            for row in parse_csv(path_to_the_file, self.COLS, self.N):
                self.userId.append(int(row[0]))
                self.movieId.append(int(row[1]))
                self.rating.append(float(row[2])) # float as a key is dangerous but with step=1/2 mb. OK
                date_time = datetime.fromtimestamp(int(row[3]))
                self.year.append(date_time.year)
        except ValueError as e:
            print(f"Wrong data format in row {len(self.userId)}: {e}")
            return
        except Exception as e:
            print(f"Processing data row {len(self.userId)} failed. {type(e).__name__}: {e}")
            return
        movies = Movies(Path(path_to_the_file).parent / 'movies.csv', 10**6)
        data = self.userId, self.movieId, self.rating, self.year
        self.movies = self.Movies(data, movies)
        self.users = self.Users(data, movies)

    class Movies:
        def __init__(self, data, movies):
            self.userId, self.movieId, self.rating, self.year = data if len(data) == 4 else ([], [], [], [])
            self.movies = movies
            self.movie_ratings = {}
            for movieId, rating in zip(self.movieId, self.rating):
                self.movie_ratings.setdefault(movieId, []).append(rating)

        def _average(self, l: list) -> float:
            return round(sum(l) / len(l), 2)
        
        def _median(self, l: list) -> float:
            n = len(l)
            sorted_l = sorted(l)
            return round(sorted_l[n//2] if n % 2 == 1 else (sorted_l[n//2] + sorted_l[n//2-1]) / 2, 2)
        
        def _pvariance(self, l: list) -> float:
            aver = self._average(l)
            return round(sum((x - aver) * (x - aver) for x in l) / len(l), 2)
        
        def _title_by_Id(self, Id: int) -> str:
            if Id in self.movies.movie_index:
                return self.movies.title[self.movies.movie_index[Id]]
        
        def _top_x_by_y(self, x, y: list, n: int, metric: str = 'average') -> dict:
            """
            Returns a dict of top-n keys (x - for example, movie)
            by the metric applied to list of values (y - for example, ratings)
            """
            calc = {'average': self._average,
                    'median': self._median,
                    'variance': self._pvariance,
                    'count': len}
            if metric not in calc:
                print(f"metric should be one of: {', '.join(calc.keys())}")
            top_list = defaultdict(list)
            for x_i, y_i in zip(x, y):
                top_list[x_i].append(y_i)
            top_metric = [(x_i, calc[metric](y_list), n_y)
                          for x_i, y_list in top_list.items()
                          if (n_y := len(y_list)) > 0]
            top_metric.sort(key = lambda x: x[1:], reverse=True) # additional sort by count (desc)
            return {item[0]: item[1] for item in top_metric[:n]}
        
        def dist_by_year(self):
            """The method returns a dict where the keys are years and the values are counts."""
            ratings_by_year = sorted(Counter(self.year).items())
            return dict(ratings_by_year)
        
        def dist_by_rating(self):
            """The method returns a dict where the keys are ratings and the values are counts."""
            ratings_distribution = sorted(Counter(self.rating).items())
            return dict(ratings_distribution)
        
        def top_by_num_of_ratings(self, n):
            """
            The method returns top-n movies by the number of ratings. 
            It is a dict where the keys are movie titles and the values are numbers.
            """
            return self.top_by_ratings(n, 'count')
        
        def top_by_ratings(self, n, metric='average'):
            """
            The method returns top-n movies by the average or median of the ratings.
            It is a dict where the keys are movie titles and the values are metric values.
            """
            x = (self._title_by_Id(Id) for Id in self.movieId)
            top_movies = self._top_x_by_y(x, self.rating, n, metric)
            return top_movies
        
        def top_controversial(self, n):
            """
            The method returns top-n movies by the variance of the ratings.
            It is a dict where the keys are movie titles and the values are the variances.
            """
            return self.top_by_ratings(n, 'variance')
        
        def movie_rating(self, movieId: int, metric='average') -> float:
            """Return movie rating by a given metric: average, median, variance, count"""
            calc = {'average': self._average,
                    'median': self._median,
                    'variance': self._pvariance,
                    'count': len}
            if metric not in calc:
                print(f"metric should be one of: {', '.join(calc.keys())}")
                return
            if movieId not in self.movie_ratings:
                print(f"movieId {movieId} not found")
                return
            return calc[metric](self.movie_ratings[movieId])

    class Users(Movies):
        def top_by_num_of_ratings(self, n):
            """
            The method returns top-n users by the number of ratings. 
            It is a dict where the keys are userIds and the values are numbers.
            """
            return self._top_x_by_y(self.userId, self.rating, n, 'count')
        
        def top_by_ratings(self, n, metric='average'):
            """
            The method returns top-n users by the average or median of the ratings.
            It is a dict where the keys are userIds and the values are metric values.
            """
            return self._top_x_by_y(self.userId, self.rating, n, metric)
        
        def top_controversial(self, n):
            """
            The method returns top-n users by the variance of the ratings.
            It is a dict where the keys are userIds and the values are the variances.
            """
            return self.top_by_ratings(n, 'variance')


class Tags:
    """
    Analyzing data from tags.csv
    """
    def __init__(self, path_to_the_file, N: int = 1000):
        self.N = N
        self.COLS = 'userId', 'movieId', 'tag', 'timestamp'
        DataRow = namedtuple('DataRow', self.COLS)
        self.userId, self.movieId, self.tag, self.date_time = [], [], [], []
        try:
            for csv_row in parse_csv(path_to_the_file, self.COLS, self.N):
                csv_data = DataRow(*csv_row)
                self.userId.append(int(csv_data.userId))
                self.movieId.append(int(csv_data.movieId))
                self.tag.append(csv_data.tag)
                self.date_time.append(datetime.fromtimestamp(int(csv_data.timestamp)))
        except ValueError as e:
            print(f"Wrong data format in row {len(self.userId)}: {e}")
            return
        except Exception as e:
            print(f"Processing data row {len(self.userId)} failed. {type(e).__name__}: {e}")
            return
        self.tag_count = Counter(self.tag) # dict of unique tags with their counts
        self.word_index = {} # {word: list_of_tag_indices}, not defaultdict simply to avoid creating new keys by a user
        for i, tag in enumerate(self.tag):
            for word in self._get_words(tag):
                 self.word_index.setdefault(word.lower(), []).append(i)

    def _shave_word(self, word: str) -> str:
        return re.sub(r'^\W+|\W+$', '', word)

    def _get_words(self, s: str) -> list:
        return list(filter(bool, map(self._shave_word, s.split())))

    def most_words(self, n: int) -> dict:
        """The method returns top-n tags with most words inside."""
        big_tags = [(tag, len(self._get_words(tag))) for tag in self.tag_count.keys()]
        big_tags.sort(key=lambda x: -x[1])
        return dict(big_tags[:n])

    def longest(self, n: int) -> list:
        """The method returns top-n longest tags in terms of the number of characters."""
        big_tags = sorted(self.tag_count.keys(), key=len, reverse=True)
        return big_tags[:n]

    def most_words_and_longest(self, n: int) -> list:
        """
        The method returns the intersection between top-n tags with most words inside and 
        top-n longest tags in terms of the number of characters.
        """
        big_tags = set(self.most_words(n).keys()) & set(self.longest(n))
        return list(big_tags)[:n]
        
    def most_popular(self, n: int) -> dict:
        """The method returns the most popular tags."""
        popular_tags = self.tag_count.most_common(n)
        return dict(popular_tags)
        
    def tags_with(self, word: str) -> list:
        """The method returns all unique tags that include the word given as the argument."""
        tags_with_word = {self.tag[i] for i in self.word_index.get(word.lower(), [])}
        return sorted(tags_with_word, key=str.lower)
        
    def tags_search(self, words: list) -> list:
        """The method returns all unique tags that include all words given as the argument."""
        indices = [set(self.word_index.get(word.lower(), [])) for word in words]
        tags_with_words = {self.tag[i] for i in reduce(set.intersection, indices)}
        return sorted(tags_with_words, key=str.lower)
        
    def popular_words(self, n: int) -> dict:
        """The method retruns nop-t post mopular wrods"""
        top_words = [(word, len(indices)) for word, indices in self.word_index.items()]
        top_words.sort(key=lambda x: -x[1])
        return dict(top_words[:n])


class Links:
    """Analyzing data from links.csv"""
    def __init__(self, path_to_the_file, N: int = 1000):
        self.N = N
        self.COLS = "movieId", "imdbId", "tmdbId"
        DataRow = namedtuple('DataRow', self.COLS)
        self.movieId, self.imdbId, self.tmdbId = [], [], []
        try:
            for csv_row in parse_csv(path_to_the_file, self.COLS, self.N):
                csv_data = DataRow(*csv_row)
                self.movieId.append(int(csv_data.movieId))
                self.imdbId.append(csv_data.imdbId or '')
                self.tmdbId.append(csv_data.tmdbId or '')
        except ValueError as e:
            print(f"Wrong data format in row {len(self.movieId)}: {e}")
            return
        except Exception as e:
            print(f"Processing data row {len(self.movieId)} failed. {type(e).__name__}: {e}")
            return
        self.movies = Movies(Path(path_to_the_file).parent / 'movies.csv', 10**6)
        self.movie_index = {movieId: i for i, movieId in enumerate(self.movieId)}
        self.avail_fields = ['Budget', 'Director', 'Directors', 'Country of origin', 'Countries of origin', 'Gross worldwide', 'Runtime', 'Stars']
        self.imdb_raw_data = {}

    def _title_by_Id(self, Id: int) -> str:
        return self.movies.title[self.movies.movie_index[Id]]

    def _parse_imdb(self, session, imdbId: str) -> dict:
        url = f"https://www.imdb.com/title/tt{imdbId}/"
        superlist = {}
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            list_items = soup.find_all('li', class_='ipc-metadata-list__item')
            for list_item in list_items:
                list_item_label = list_item.find(class_='ipc-metadata-list-item__label')
                if list_item_label:
                    list_item_content = list_item.find_all(class_='ipc-metadata-list-item__list-content-item')
                    superlist[list_item_label.text] = [item.text for item in list_item_content if item]
        except RequestException as e:
            print(f"Request error while parsing imdbId = {imdbId}: {e}")
        except Exception as e:
            print(f"Unknown error while parsing imdbId = {imdbId}: {e}")
        finally:
            return superlist
    
    def get_imdb(self, list_of_movies, list_of_fields) -> list:
        """
        The method returns a list of lists [movieId, field1, field2, field3, ...] 
        for the list of movies given as the argument (movieId).
        """
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        for field in list_of_fields:
            if field not in self.avail_fields:
                print(f"Available fields: {', '.join(self.avail_fields)}")
                return []
        imdb_info = []
        with requests.Session() as session:
            session.headers.update(headers)
            for movieId in list_of_movies:
                if movieId not in self.movie_index:
                    continue
                imdbId = self.imdbId[self.movie_index[movieId]]
                # Caching raw data for future requests:
                if imdbId in self.imdb_raw_data:
                    raw_data = self.imdb_raw_data[imdbId]
                else:
                    self.imdb_raw_data[imdbId] = raw_data = self._parse_imdb(session, imdbId)
                data_row = [movieId]
                for field in list_of_fields:
                    field_value = raw_data.get(field, [])
                    if field in ('Director', 'Directors'):
                        field_value = raw_data.get('Director', []) or raw_data.get('Directors', [])
                    elif field in ('Country of origin', 'Countries of origin'):
                        field_value = raw_data.get('Country of origin', []) or raw_data.get('Countries of origin', [])
                    elif field in ('Budget', 'Gross worldwide'):
                        match = re.search(r"\$[0-9,]+", (field_value or [''])[0])
                        field_value = int(match.group().replace('$', '').replace(',', '') if match else '0')
                    elif field == 'Runtime':
                        minutes = 0
                        for amount, unit in re.findall(r'(\d+)\s*([hm])', (field_value or [''])[0]):
                            minutes += int(amount) * {"m": 1, "h": 60}[unit]
                        field_value = minutes
                    data_row.append(field_value)
                imdb_info.append(data_row)
        return sorted(imdb_info, key=lambda x: -x[0])
        
    def top_directors(self, n: int) -> dict:
        """
        The method returns a dict with top-n directors where the keys are directors and 
        the values are numbers of movies created by them.
        """
        all_directors = self.get_imdb(self.movieId, ['Director'])
        directors = Counter(director for movie_directors in all_directors
                            for director in movie_directors[1]).most_common(n)
        return dict(directors)
        
    def most_expensive(self, n: int) -> dict:
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are their budgets.
        """
        budgets_by_id = self.get_imdb(self.movieId, ['Budget'])
        budgets = [(self._title_by_Id(movieId), budget)
                   for (movieId, budget) in budgets_by_id
                   if movieId in self.movies.movieId]
        return dict(sorted(budgets, key = lambda x: -x[1])[:n])
        
    def most_profitable(self, n: int) -> dict:
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are the difference between cumulative worldwide gross and budget.
        """
        profits_by_id = self.get_imdb(self.movieId, ['Budget', 'Gross worldwide'])
        profits = [(self._title_by_Id(movieId), gross - budget)
                   for (movieId, budget, gross) in profits_by_id
                   if movieId in self.movies.movieId and gross != 0 and budget != 0]
        return dict(sorted(profits, key = lambda x: -x[1])[:n])
        
    def longest(self, n: int) -> dict:
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are their runtime.
        """
        runtimes_by_id = self.get_imdb(self.movieId, ['Runtime'])
        runtimes = [(self._title_by_Id(movieId), runtime)
                    for (movieId, runtime) in runtimes_by_id
                    if movieId in self.movies.movieId]
        return dict(sorted(runtimes, key = lambda x: -x[1])[:n])
        
    def top_cost_per_minute(self, n: int) -> dict:
        """
        The method returns a dict with top-n movies where the keys are movie titles and
        the values are the budgets divided by their runtime.
        """
        costs_by_id = self.get_imdb(self.movieId, ['Budget', 'Runtime'])
        costs = [(self._title_by_Id(movieId), round(budget / runtime, 2))
                 for (movieId, budget, runtime) in costs_by_id
                 if movieId in self.movies.movieId and runtime != 0]
        return dict(sorted(costs, key = lambda x: -x[1])[:n])
    
    def top_countries(self, n: int) -> dict:
        """The method returns a dict with top-n countries by number of movies"""
        all_countries = self.get_imdb(self.movieId, ['Country of origin'])
        countries = Counter(country for movie_countries in all_countries
                            for country in movie_countries[1]).most_common(n)
        return dict(countries)
    
    def top_stars(self, n: int) -> dict:
        """The method returns a dict with top-n stars by number of movies"""
        all_stars = self.get_imdb(self.movieId, ['Stars'])
        stars = Counter(star for movie_stars in all_stars
                            for star in movie_stars[1]).most_common(n)
        return dict(stars)


class Tests:
    @classmethod
    def setup_class(cls):
        cls.movies = Movies('../datasets/movies.csv', 10000)
        cls.ratings = Ratings('../datasets/ratings.csv', 101000)
        cls.tags = Tags('../datasets/tags.csv', 10000)
        cls.links = Links('../datasets/links.csv', 10)
        cls.movies_small = Movies('../datasets/movies.csv', 10)
        cls.ratings_small = Ratings('../datasets/ratings.csv', 10)
        cls.tags_small = Tags('../datasets/tags.csv', 10)


    """Movies tests"""
    def test_movie_data_amount(self):
        assert len(self.movies.title)==len(self.movies.genres) == len(self.movies.movieId) == len(self.movies.year)
        
    def test_movie_genres_types(self):
        n = int(randint(1, len(self.movies.movieId))) - 1
        data  = [self.movies.movieId[n],self.movies.title[n], self.movies.genres[n], self.movies.year[n]]
        result = True
        check_types = (
            isinstance(data[0], int) and
            isinstance(data[1], str) and
            isinstance(data[2], list) and
            (isinstance(data[3], int) or data[3] is None)
            )
        if not check_types:
            print([type(d) for d in data])
            result = False
        assert result

    def test_most_genres(self):
        n = 1000
        data = self.movies.most_genres(n)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), int)
        assert list(data.values())[0] >= list(data.values())[-1]
        assert self.movies_small.most_genres(1) == {'Toy Story (1995)': 5}

    def test_dist_by_release(self):
        n = 10000
        data = list((res := self.movies.dist_by_release()).items())[:n]
        assert ((data[0][1] >= data[-1][1]) and isinstance(res, dict)
                and (isinstance(data[0][0],int) and isinstance(data[-1][0],int)))
        assert self.movies_small.dist_by_release() == {1995: 10}

    def test_dist_by_genres(self):
        data = self.movies.dist_by_genres()
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), int)
        assert list(data.values())[0] >= list(data.values())[-1]
        assert list(self.movies_small.dist_by_genres().items())[0] == ('Comedy', 5)

    def test_title_search(self):
        search1 = ['Lethal', 'Weapon']
        search2 = ['Dune', 'Part', 'Three']
        data1 = self.movies.title_search(search1)
        data2  = self.movies.title_search(search2)
        assert isinstance(data1, dict)
        assert isinstance(choice(list(data1.keys())), int)
        assert isinstance(choice(list(data1.values())), str)
        assert len(data1) == 4
        assert len(data2) == 0

    """Ratings tests"""
    def test_dist_by_year(self):
        data = self.ratings.movies.dist_by_year()
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), int)
        assert list(data.keys())[0] <= list(data.keys())[-1]
        assert self.ratings_small.movies.dist_by_year() == {2000: 10}

    def test_dist_by_rating(self):
        data = self.ratings.movies.dist_by_rating()
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), float) or isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), int)
        assert list(data.keys())[0] <= list(data.keys())[-1]
        assert self.ratings_small.movies.dist_by_rating() == {3: 1, 4: 4, 5: 5}

    def test_top_by_num_of_ratings(self):
        n = 10000
        data = self.ratings.movies.top_by_num_of_ratings(n)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.movies.top_by_num_of_ratings(10).values()) == [1] * 10

    def test_top_by_ratings(self):
        n = randint(1, self.ratings.N) - 1
        data = self.ratings.movies.top_by_ratings(n)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.movies.top_by_ratings(5).values()) == [5] * 5

    def test_top_by_ratings_median(self):
        n = 10000
        data = self.ratings.movies.top_by_ratings(n, 'median')
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.movies.top_by_ratings(5, 'median').values()) == [5] * 5

    def test_top_controversial(self):
        n = randint(1, self.ratings.N) - 1
        data = self.ratings.movies.top_controversial(n)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.movies.top_controversial(10).values()) == [0] * 10

    """User"""
    def test_top_by_num_of_ratings_user(self):
        data = self.ratings.users.top_by_num_of_ratings(10)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.users.top_by_num_of_ratings(1).values()) == [10]

    def test_top_by_ratings_user(self):
        data = self.ratings.users.top_by_ratings(10)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.users.top_by_ratings(1).values()) == [4.4]

    def test_top_by_ratings_median_user(self):
        data = self.ratings.users.top_by_ratings(10, 'median')
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.users.top_by_ratings(1, 'median').values()) == [4.5]

    def test_top_controversial_user(self):
        data = self.ratings.users.top_controversial(10)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), int)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert list(self.ratings_small.users.top_controversial(1).values()) == [0.44]


    """Tags"""
    def test_tags_data_amount(self):
        len(self.tags.movieId) == len(self.tags.userId) == len(self.tags.tag) == len(self.tags.date_time)

    def test_most_words(self):
        data = self.tags.most_words(10)
        assert isinstance(data, dict)
        for word, length in list(data.items()):
            assert isinstance(word, str) and isinstance(length, int)
            word_len = len(self.tags._get_words(word))
            assert word_len == length
        assert self.tags_small.most_words(1) == {'way too long': 3}

    def test_popular_words(self):
        data = self.tags.popular_words(10)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert data['netflix'] == 134

    def test_longest(self):
        data = self.tags.longest(10)
        assert isinstance(data, list)
        assert isinstance(choice(data), str)
        assert len(data[0]) >= len(data[-1])
        assert self.tags_small.longest(1) == ['Leonardo DiCaprio']

    def test_most_words_and_longest(self):
        n = 10
        data = self.tags.most_words_and_longest(n)
        test_data = list(set( set(self.tags.most_words(n).keys()) ) & ( set(self.tags.longest(n)) ))
        assert isinstance(data, list)
        assert isinstance(choice(data), str)
        assert (data[0] == data[0]) and (data[-1] == test_data[-1])
        assert len(self.tags_small.most_words_and_longest(7)) == 7

    def test_most_popular(self):
        data = self.tags.most_popular(10)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.keys())), str)
        assert isinstance(choice(list(data.values())), int)
        assert data[list(data.keys())[0]] >= data[list(data.keys())[-1]]
        assert data['In Netflix queue'] == 131

    def test_tags_with(self):
        markers =["ending","sdD#23f3"]
        for marker in markers:
            data = self.tags.tags_with(marker.lower())
            assert isinstance(data, list)
            for word in data:
                assert isinstance(word, str)
                if marker in word.lower():
                    assert len(data)>0
                else: assert len(data)==0

    def test_tag_search(self):
        data_1 = self.tags.tags_search(['comedy', 'black'])
        data_2 = self.tags.tags_search(['dgdffggdg', 'sfd'])
        assert isinstance(data_1,list)
        assert isinstance(choice(data_1), str)
        assert isinstance(data_2,list)
        assert len(data_1) > 0
        assert len(data_2) == 0


    """Links test"""
    def test_avail_fields(self):
        data = self.links.avail_fields
        test_fields = ['Budget', 'Director', 'Directors', 'Country of origin',
                       'Countries of origin', 'Gross worldwide', 'Runtime', 'Stars']
        assert isinstance(data, list)
        assert isinstance(choice(data), str)
        assert set(data) == set(test_fields)

    def test_get_imdb(self):
        n = 5
        movieIds = [choice(self.links.movieId) for _ in range(n)]
        movie_titles = {movieId: self.links.movies.title[self.links.movies.movie_index[movieId]] for movieId in movieIds}
        data = [[movie_titles[movieId], data] for movieId, *data in self.links.get_imdb(movieIds, self.links.avail_fields)]
        for movieId, *data in self.links.get_imdb(movieIds, self.links.avail_fields):
            assert len(data) == 8
            assert all(isinstance(data[i], t) for i, t in enumerate([int, list, list, list, list, int, int, list]))
            assert all(isinstance(item, str) for item in choice(data[1:5] + data[7]))
        assert 'Tom Hanks' in self.links.get_imdb([1], ['Stars'])[0][1]

    def test_top_directors(self):
        data = self.links.top_directors(10)
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert len(data) == 10 and 'Martin Campbell' in data

    def test_most_expensive(self):
        data = self.links.most_expensive(5)
        assert isinstance(data,dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert list(data.values())[0] == 65_000_000

    def test_most_profitable(self):
        data = self.links.most_profitable(5)
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert 'Toy Story' in list(data.keys())[0]

    def test_longest(self):
        n = 5
        data = self.links.longest(n)
        assert len(data) == n
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert 'Heat' in list(data.keys())[0]

    def test_top_cost_per_minute(self):
        data = self.links.top_cost_per_minute(5)
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],float)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert 'Jumanji' in list(data.keys())[0]

    def test_top_countries(self):
        data = self.links.top_countries(15)
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert 'United States' in list(data.keys())[0]

    def test_top_stars(self):
        data = self.links.top_stars(5)
        assert isinstance(data, dict)
        assert all((isinstance(item[0],str) and isinstance(item[1],int)) for item in data.items())
        assert list(data.values())[0] >= list(data.values())[-1]
        assert list(data.values())[0] == 1


    """Additional tests"""
    def test_avarege(self):
        test_1 = [2, 4, 5]
        assert (res := self.ratings.movies._average(test_1)) == round(sum(test_1) / len(test_1), 2)
        assert isinstance(res, float)

    def test_median(self):
        assert (res := self.ratings.movies._median([4, 2, 5])) == 4
        assert isinstance(res, int) or isinstance(res, float)

    def test_pvariance(self):
        assert (res := self.ratings.movies._pvariance([2, 5, 4])) == 1.56
        assert isinstance(res, float)

    def test_title_by_Id(self):
        id = 1 #Toy Story (1995)
        title = self.ratings.movies._title_by_Id(id)
        assert title == "Toy Story (1995)"
        assert isinstance(title, str)

    def test_shave_word(self):
        word_1 = "    word_1"
        word_2 = "word_2   "
        assert self.tags._shave_word(word_1) == "word_1"
        assert (res := self.tags._shave_word(word_2)) == "word_2"
        assert isinstance(res, str)

    def test_get_words(self):
        words = "1 2 3"
        res = self.tags._get_words(words)
        assert res == ['1', '2', '3']
        assert isinstance(res, list)
        assert isinstance(choice(res), str)

    def test_links_title_by_Id(self):
        id_ = 1
        title = self.links._title_by_Id(id_)
        assert isinstance(title, str)
        assert title == "Toy Story (1995)"

    def test_parse_imdb(self):
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        with requests.Session() as session:
            session.headers.update(headers)
            fields = self.links._parse_imdb(session, '15239678')
        assert isinstance(fields, dict)
        assert fields.get('Director') and fields.get('Budget') and fields.get('Stars')
        assert isinstance(fields['Director'], list) and isinstance(fields['Budget'], list) and isinstance(fields['Stars'], list)
        assert isinstance(fields['Director'][0], str) and isinstance(fields['Budget'][0], str) and isinstance(fields['Stars'][0], str)
        assert 'Denis Villeneuve' in fields['Director']

    def test_top_x_by_y(self):
        n = 10
        data = self.ratings.movies._top_x_by_y(self.ratings.movieId, self.ratings.rating, n)
        assert isinstance(data, dict)
        assert isinstance(choice(list(data.values())), float) or isinstance(choice(list(data.values())), int)
        assert len(data) == n
        assert list(self.ratings_small.movies._top_x_by_y(self.ratings_small.movieId, self.ratings_small.rating, 5).values()) == [5] * 5

    def test_qshave(self):
        assert (res1 := qshave('"preved"')) == 'preved'
        assert qshave("'medved'") == "'medved'"
        assert isinstance(res1, str)

    def test_qsplit(self):
        s1 = '"preved, medved","medved","preved",123456'
        assert (split_str := qsplit(s1)) == ['"preved, medved"', '"medved"', '"preved"', '123456']
        assert isinstance(split_str, list)
        assert isinstance(choice(split_str), str)
        with pytest.raises(ValueError):
            qsplit('"preved,"medved"')

    def test_parse_csv(self):
        for row in parse_csv('../datasets/movies.csv', ['movieId', 'title', 'genres']):
            assert isinstance(row, list)
            assert len(row) == 3
            assert all(isinstance(value, str) for value in row)
        for row in parse_csv('../datasets/movies.csv', ['movieId', 'title', 'genres'], 1):
            assert row[0] == '1'
        with pytest.raises(ValueError):
            for _ in parse_csv('../datasets/tags.csv', ['movieId', 'title', 'genres'], 1):
                pass

    def test_attr_list(self):
        movies_attrs = attr_list(self.movies)
        assert isinstance(movies_attrs, list)
        assert isinstance(choice(movies_attrs), str)
        assert set(movies_attrs) == {'N', 'COLS', 'movieId', 'title', 'genres', 'year', 'movie_index'}

    def test_movie_rating(self):
        assert (res := self.ratings_small.movies.movie_rating(1)) == 4.0
        assert isinstance(res, float)

