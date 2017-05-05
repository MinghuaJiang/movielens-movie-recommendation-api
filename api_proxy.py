import urllib2
import json

def get_more_movie_info(input_result):
    with open("tmdb_secret.json") as input:
        config = json.load(input)

    movie_url_root = "https://api.themoviedb.org/3/movie/"+str(input_result['tmdbId'])+"?api_key="+config['access_key']
    video_url_root = "https://api.themoviedb.org/3/movie/"+str(input_result['tmdbId'])+"/videos?api_key="+config['access_key']
    result = json.load(urllib2.urlopen(movie_url_root))
    video_result = json.load(urllib2.urlopen(video_url_root))
    input_result["image_url"] = "http://image.tmdb.org/t/p/w300"+result['poster_path']
    input_result["introduction"] = result['overview']
    if video_result is not None and len(video_result['results']) > 0:
        input_result["youtube_url"] = "http://www.youtube.com/embed/" + video_result['results'][0]['key']
    return input_result


if __name__=='__main__':
    input = dict()
    input['tmdbId'] = 862
    get_more_movie_info(input)