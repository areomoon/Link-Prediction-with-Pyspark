# Link Prediction in Twitter Network with Pyspark

## Data Resource
Dataset: **Twitter-Dynamic-Net**  

Link: https://aminer.org/data-sna  

It's a crawled a twitter dataset,which selected the most popular user on Twitter, i.e., **“Lady Gaga”**, and randomly collected 10,000 of her followers. 
Take these users as seed users and used a crawler to collect all followers of these users by traversing “following” relationships 
and these followers are viewed as the user list and the total number is 112,044. 
The crawler monitored the change of the network structure among the 112,044 users from 10.12.2010 to 12.23.2010 and 
obtained 443,399 dynamic “following” relationships between them.  



## Introduction
We use Spark APIs to implement Matrix Factorizationan,Alternating Least Squares (ALS) algorithm and multiple Machine learning algorithm to predict the links between tweet users. 


## Requirements/Packages
- networkx
- Python3.x
- findspark
- pyspark
- pandas
- numpy 


## How to use the files?  
### 1.Get preprocessed data 
Execute the `get_data.py` to extract and create multiple similarity score features (**Adamic similarity, Jaccard similarity, Pagerank, Cosine similarity** on tweet text) from the tweet dynamic net dataset. The files would be stored in the `tweet_data` file  
`python get_data.py`  

**P.S: For the file 'tweet_result_url_2011_tag_n_username.csv' used in the function, please refer to the following link:  **

### 2.Predict the link by using **ALS algorithm**  
**ALS:** Collaborative filtering is commonly used for recommender systems. These techniques aim to fill in the missing entries of a user-item association matrix. spark.ml currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries. spark.ml uses the alternating least squares (ALS) algorithm to learn these latent factors.   

Execute the `ALS_classifier.py` to generate forecast and evaluation by using the `tweet_data/grapg_cb.txt` file  
`python ALS_classifier.py`  

### 3.Predict the link by using **RandomForest algorithm**  
Execute the `ml_RF_classifier.py` to generate forecast by using the similarity features we get from previous calculation and do evaluation (F1-score)  
`python ml_RF_classifier.py`  


