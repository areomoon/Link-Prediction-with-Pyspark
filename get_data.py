from tweet_utils import get_balance_data as bal_dat
from tweet_utils import get_sim_score as sim

def main():
    bal_dat.get_balance_data('tweet_data/graph_cb.txt')   # get balanced data
    # sim.get_adamic('tweet_data/graph_cb_balanced.csv') still fixing bugs
    sim.get_IPA_cluster('tweet_data/graph_cb_balanced.csv') # get IPA data
    sim.get_pagerank('tweet_data/graph_cb_balanced.csv') # get Pagerank score
    sim.get_jaccard('tweet_data/graph_cb_balanced_withPRscore.csv')  # get Jaccard similarity


if __name__ == "__main__":
    main()