from tweet_utils import get_balance_data as bal_dat
from tweet_utils import get_sim_score as sim

def main():
    bal_dat.get_balance_data('tweet_data/graph_cb.txt')   # get balanced data
    sim.get_adamic('tweet_data/graph_cb_balanced.csv') # get adamic similarity
    sim.get_IPA_cluster('tweet_data/graph_cb_balanced.csv')  # get IPA data
    sim.get_pagerank('tweet_data/graph_cb_balanced.csv') # get Pagerank score
    sim.get_jaccard('tweet_data/tweet_pagerank.csv')  # get Jaccard similarity
    sim.get_cosine('tweet_data/tweet_result_url_2011_tag_n_username.csv') # get cosine similarity based on the tweet content data

if __name__ == "__main__":
    main()