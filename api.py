import logging
import os
from flask import Flask, request
import rate_limit_redis
from engine import RecommendationEngine
import json
from dao import MovieRatingDao
from dao import MovieInfoDao
import dataset_updater
from pyspark import SparkContext, SparkConf
import api_proxy

api = Flask(__name__)
logger = logging.getLogger(__name__)


@api.route("/api/v1/recommendation/<user_id>/<int:count>", methods=["GET"])
def top_recommendation(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_personalized_recommendation(user_id, count)
    return json.dumps(top_ratings)


@api.route("/api/v1/recommendation/rating/<int:count>", methods=["GET"])
def top_average_rating(count):
    top_ratings = recommendation_engine.get_top_average_ratings(count)
    return json.dumps(top_ratings)


@api.route("/api/v1/recommendation/popular/<int:count>", methods=["GET"])
def most_popular(count):
    top_ratings = recommendation_engine.get_most_popular(count)
    return json.dumps(top_ratings)


@api.route("/api/v1/prediction/<user_id>/<int:movie_id>", methods=["GET"])
def predict_rating(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_predict_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)


@api.route("/api/v1/rating/<user_id>/<int:movie_id>/<float:rating>", methods=["GET", "POST"])
def add_comment_and_rating(user_id, movie_id, rating):
    # get the ratings from the Flask POST request object
    # user_id = 0
    # movie_id = request.form.movie_id
    # comments = request.form.comment
    # rating = request.form.rating
    if not movie_dao.check_if_rated(user_id, movie_id):
        movie_dao.add_comments_rating(user_id, movie_id, "", rating)
        recommendation_engine.add_rating([(user_id, movie_id, rating)])
        return "added rating successfully"
    else:
        return "already rated"


@api.route("/api/v1/rating/<user_id>", methods=["GET"])
def get_rated_movie_count(user_id):
    result = dict()
    result["count"] = movie_dao.get_rated_movie_count(user_id)
    return json.dumps(result)


@api.route("/api/v1/rating/average/<int:movie_id>", methods=["GET"])
def average_rating_count(movie_id):
    top_ratings = recommendation_engine.get_average_rating_count(movie_id)
    return json.dumps(top_ratings)


@api.route("/api/v1/rating/<user_id>/<int:movie_id>", methods=["GET"])
def check_rated(user_id, movie_id):
    result = dict()
    result['rated'] = movie_dao.check_if_rated(user_id, movie_id)
    return json.dumps(result)


@api.route("/api/v1/comment/<user_id>/<int:movie_id>", methods=["GET"])
def get_my_comment(user_id, movie_id):
    result = movie_dao.get_my_comment(user_id, movie_id)
    return json.dumps(result)


@api.route("/api/v1/comments/<user_id>/<int:movie_id>", methods=["GET"])
def get_other_comments(user_id, movie_id):
    result = movie_dao.get_all_other_comments(user_id, movie_id)
    return json.dumps(result)


@api.route("/api/v1/movie_info/<int:movie_id>", methods=["GET"])
def get_movie_info(movie_id):
    result = movie_info_dao.get_movie_info(movie_id)
    result = api_proxy.get_more_movie_info(result)
    return json.dumps(result)


@api.route("/api/v1/model", methods=["GET", "POST"])
@rate_limit_redis.ratelimit(limit=1, per=60*2)
def retrain_model():
    recommendation_engine.retrain_model()
    return "model re-trained successfully"


@api.route("/api/v1/datasets", methods=["GET", "POST"])
@rate_limit_redis.ratelimit(limit=1, per=60*2)
def update_dataset():
    if dataset_updater.update_dataset():
        recommendation_engine.load_dataset(movie_dao.get_all_user_ratings())
        return "dataset updated successfully"
    else:
        return "no changes in dataset"


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movielens_recommendation").setMaster("spark://spark-master:7077").set(
        "spark.executor.memory", "4g")
    # IMPORTANT: pass additional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py'])
    return sc


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    sc = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest-small')
    movie_dao = MovieRatingDao()
    movie_info_dao = MovieInfoDao()

    recommendation_engine = RecommendationEngine(sc, dataset_path, movie_dao.get_all_user_ratings())

    api.debug = False
    api.run(host="spark-master")
