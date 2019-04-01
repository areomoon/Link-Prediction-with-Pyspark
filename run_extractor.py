

from tweet_utils import tweet_content_extractor as ext
import sys

assert sys.version_info >= (3, 0)

ext.tweet_extractor('2011_01_15').get_tweet_content_files()
ext.tweet_extractor('2011_01_15').get_tweet_url_files() # extract url_tag tweet without words files(tweet data)
