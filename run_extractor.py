

from tweet_utils import tweet_content_extractor as ext
import sys
# replace load_file with your local file directory
'''
get_tweet_content_files()  is to save [user,[tweet_content]] as .csv file

get_tweet_url_files()      is to save [user,[web_link/ hashname_tag]] as .csv file
'''
assert sys.version_info >= (3, 0)
ext.tweet_extractor('2011_01_15').get_tweet_content_files()
ext.tweet_extractor('2011_01_15').get_tweet_url_files()
