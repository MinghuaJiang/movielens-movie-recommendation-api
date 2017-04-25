import logging
import os
from flask import Flask, request
from engine import RecommendationEngine
import json
from dao import MovieRatingDao
import dataset_updater
from pyspark import SparkContext, SparkConf

application = Flask(__name__)
logger = logging.getLogger(__name__)


@application.route("/<user_id>/recommendation/<int:count>", methods=["GET"])
def top_recommendation(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_personalized_recommendation(user_id, count)
    return json.dumps(top_ratings)


@application.route("/<user_id>/predict/<int:movie_id>", methods=["GET"])
def predict_rating(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_predict_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)


@application.route("/rating/<user_id>/<int:movie_id>/<float:rating>", methods=["GET", "POST"])
def add_comment_and_rating(user_id, movie_id, rating):
    # get the ratings from the Flask POST request object
    #user_id = 0
    #movie_id = request.form.movie_id
    #comments = request.form.comment
    #rating = request.form.rating
    if not recommendation_engine.check_if_rated(user_id, movie_id):
        dao.add_comments_rating(user_id, movie_id, "", rating)
        recommendation_engine.add_rating([(user_id, movie_id, rating)])
        return "added rating successfully"
    else:
        return "already rated"


@application.route("/rating/<user_id>/<int:movie_id>", methods=["GET"])
def check_rated(user_id, movie_id):
    result = dict()
    result['rated'] = recommendation_engine.check_if_rated(user_id, movie_id)
    return json.dumps(result)


@application.route("/model", methods=["GET", "POST"])
def retrain_model():
    recommendation_engine.retrain_model()
    return "model re-trained successfully"


@application.route("/datasets", methods=["GET", "POST"])
def update_dataset():
    if dataset_updater.update_dataset():
        recommendation_engine.load_dataset(dao.get_user_ratings())
        return "dataset updated successfully"
    else:
        return "no changes in dataset"


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movielens_recommendation").setMaster("spark://spark-master:7077")
    # IMPORTANT: pass additional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py'])
    return sc

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    sc = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest-small')
    dao = MovieRatingDao()
    recommendation_engine = RecommendationEngine(sc, dataset_path, dao.get_user_ratings())

    application.debug = False
    application.run()

