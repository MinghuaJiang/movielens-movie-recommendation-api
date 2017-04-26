import os

from pyspark.mllib.recommendation import ALS

import logging
import itertools
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable)
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1])) / nratings)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __init__(self, sc, dataset_path, user_ratings):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.sc = sc
        self.dataset_path = dataset_path
        self.load_dataset(user_ratings)

    def __init_rating_rdd(self, user_ratings):
        ratings_file_path = os.path.join(self.dataset_path, 'ratings.csv')
        ratings_raw_RDD = self.sc.textFile(ratings_file_path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line != ratings_raw_data_header) \
            .map(lambda line: line.split(",")).map(
            lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()
        if len(user_ratings) > 0:
            new_ratings_RDD = self.sc.parallelize(user_ratings)
            # Add new ratings to the existing ones
            self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)

    def __init_movie_rdd(self):
        movies_file_path = os.path.join(self.dataset_path, 'movies.csv')
        movies_raw_RDD = self.sc.textFile(movies_file_path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line != movies_raw_data_header) \
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]), tokens[1], tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]), x[1])).cache()

    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from
        the current data self.ratings_RDD
        """
        logger.info("Counting movie ratings...")
        movie_ID_with_ratings_RDD = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        movie_ID_with_avg_ratings_RDD = movie_ID_with_ratings_RDD.map(get_counts_and_averages)
        self.movies_rating_counts_RDD = movie_ID_with_avg_ratings_RDD.map(lambda x: (x[0], x[1][0]))

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        training_RDD, validation_RDD, test_RDD = self.ratings_RDD.randomSplit([6, 2, 2], seed=0L)
        validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
        test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

        ranks = [8, 12]
        lambdas = [1.0, 10.0]
        numIters = [10, 20]
        bestModel = None
        bestValidationRmse = float("inf")
        bestRank = 0
        bestLambda = -1.0
        bestNumIter = -1

        for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
            model = ALS.train(training_RDD, rank, numIter, lmbda)
            predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
            rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
            validationRmse = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())
            logger.info(
                "RMSE (validation) = %f for the model trained with rank = %d, lambda = %.1f, and numIter = %d." % (
                    validationRmse, rank, lmbda, numIter))

            if validationRmse < bestValidationRmse:
                bestModel = model
                bestValidationRmse = validationRmse
                bestRank = rank
                bestLambda = lmbda
                bestNumIter = numIter

        predictions = bestModel.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
        rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        testRmse = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean())

        # evaluate the best model on the test set
        logger.info(
            "The best model was trained with rank = %d and lambda = %.1f, and numIter = %d, and its RMSE on the test set is %f." % (
                bestRank, bestLambda, bestNumIter, testRmse))

        self.bestModel = bestModel
        self.bestValidationRmse = bestValidationRmse
        self.bestRank = bestRank
        self.bestLambda = bestLambda
        self.bestNumIter = bestNumIter
        # Train the model
        logger.info("ALS model built!")

    def __predict_ratings(self, user_and_movie_RDD):
        """Gets predictions for a given (userID, movieID) formatted RDD
        Returns: an RDD with format (movieTitle, movieRating, numRatings)
        """
        predicted_RDD = self.bestModel.predictAll(user_and_movie_RDD)
        predicted_rating_RDD = predicted_RDD.map(lambda x: (x.product, x.rating))
        predicted_rating_title_and_count_RDD = \
            predicted_rating_RDD.join(self.movies_titles_RDD).join(self.movies_rating_counts_RDD)
        predicted_rating_title_and_count_RDD = \
            predicted_rating_title_and_count_RDD.map(lambda r: (r[1][0][1], r[1][0][0], r[1][1]))

        return predicted_rating_title_and_count_RDD

    def retrain_model(self):
        logger.info("Re-Training the ALS model for newly added user rating...")
        self.bestmodel = ALS.train(self.ratings_RDD, self.bestRank, self.bestNumIter, self.bestLambda)
        logger.info("ALS model built!")

    def add_rating(self, rating):
        """Add additional movie rating in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings_RDD = self.sc.parallelize(rating)

        # Add new ratings to the existing ones
        self.ratings_RDD = self.ratings_RDD.union(new_ratings_RDD)
        # Re-compute movie ratings count
        self.__count_and_average_ratings()

    def get_predict_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        requested_movies_RDD = self.sc.parallelize(movie_ids).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_movies_RDD).collect()

        return ratings

    def get_top_personalized_recommendation(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        # Get pairs of (userID, movieID) for user_id unrated movies
        user_unrated_movies_RDD = self.ratings_RDD.filter(lambda rating: not rating[0] == user_id).map(
            lambda x: (user_id, x[1])).distinct()
        # Get predicted ratings
        ratings = self.__predict_ratings(user_unrated_movies_RDD).filter(lambda r: r[2] >= 25).takeOrdered(movies_count,
                                                                                                           key=lambda
                                                                                                               x: -x[1])
        return ratings

    def get_top_average_ratings(self, movies_count):
        ratings = self.movies_rating_counts_RDD.filter(lambda r: r[1] >= 25).takeOrdered(movies_count,
                                                                                         key=lambda x: -x[2])
        return ratings

    def get_most_popular(self, movies_count):
        ratings = self.movies_rating_counts_RDD.filter(lambda r: r[1] >= 25).takeOrdered(movies_count,
                                                                                         key=lambda x: -x[2])
        return ratings


    def check_if_rated(self, user_id, movie_id):
        rating_RDD = self.ratings_RDD.filter(lambda x: x[0] == user_id).filter(lambda x: x[1] == movie_id)
        return len(rating_RDD.collect()) > 0

    def load_dataset(self, user_ratings):
        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        self.__init_rating_rdd(user_ratings)
        # Load movies data for later use
        logger.info("Loading Movies data...")
        self.__init_movie_rdd()
        # Pre-calculate movies ratings counts
        self.__count_and_average_ratings()
        self.__train_model()
