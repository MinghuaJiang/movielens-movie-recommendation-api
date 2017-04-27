from pymongo import MongoClient
import datetime


class MovieRatingDao:
    def __init__(self):
        self.db = MongoClient('184.73.28.43', 27017)['movie-lens']

    def add_comments_rating(self, user_id, movie_id, comments, rating):
        result = self.db.movie.find_one({'movie_id': movie_id})
        if result is None:
            self.db.movie.insert({'movie_id': movie_id, 'comments': [
                {'comment': comments, 'rating': rating, 'comment_by': user_id,
                 'comment_time': datetime.datetime.utcnow()}]})
        else:
            self.db.movie.update({'movie_id': movie_id},
                                 {'$push': {'comments':
                                                {'comment': comments, 'rating': rating, 'comment_by': user_id,
                                                 'comment_time': datetime.datetime.utcnow()
                                                 }
                                            }
                                  })

    def get_all_other_comments(self, user_id, movie_id):
        db_result = self.db.movie.find_one({'movie_id': movie_id}, {'movie_id': 0, '_id': 0})
        result = dict()
        result["comments"] = [i for i in db_result["comments"] if i["comment_by"] != user_id]
        result["comments"].reverse()
        return result

    def get_my_comment(self, user_id, movie_id):
        result = self.db.movie.find_one({'movie_id': movie_id, 'comments.comment_by': user_id},
                                        {'comments.$': 1, '_id': 0})
        return result["comments"][0]

    def get_all_user_ratings(self):
        result = []
        db_result = self.db.movie.find()
        for each in db_result:
            for comment in each["comments"]:
                result.append((comment["comment_by"], each["movie_id"], comment["rating"]))
        return result

    def check_if_rated(self, user_id, movie_id):
        result = self.db.movie.find_one({'movie_id': movie_id, 'comments.comment_by': user_id})
        return result is not None

    def get_rated_movie_count(self, user_id):
        result = self.db.movie.find({'comments.comment_by': user_id}).count()
        return result


class MovieInfoDao:
    def __init__(self):
        self.db = MongoClient('184.73.28.43', 27017)['movie-lens']
        self.bulk = self.db.movie_info.initialize_ordered_bulk_op()

    def truncate_table(self):
        self.db.movie_info.remove({})

    def bulk_insert(self):
        self.bulk.execute()

    def add_movie_info(self, movie_id, movie_title, genres, imdbId, tmdbId, bulk=True):
        if bulk:
            self.bulk.insert(
            {'movie_id': movie_id, 'movie_title': movie_title, 'genres': genres, 'imdbId': imdbId, 'tmdbId': tmdbId})
        else:
            self.db.movie_info.insert(
                {'movie_id': movie_id, 'movie_title': movie_title, 'genres': genres, 'imdbId': imdbId,
                 'tmdbId': tmdbId})

    def get_movie_info(self, movie_id):
        result = self.db.movie_info.find_one({'movie_id': movie_id},{'_id', 0})
        return result

if __name__ == '__main__':
    dao = MovieRatingDao()
    # dao.add_comments_rating("ben2", 1, 'test', '5')
    # dao.add_comments_rating("ben3", 1, 'test4', '4')
    # dao.get_all_user_ratings()
    print dao.check_if_rated('ben3', 1)
