import networkx as nx
import pandas as pd
import os
import re

def get_PR_score_with_relationship(filepath,alpha=0.9,save_file=True):
    D = nx.DiGraph()
    new_graph_df=pd.read_csv(filepath)
    new_graph_df_with_rl=new_graph_df[new_graph_df['follow']==1]
    nodes_uniq = set(pd.concat([new_graph_df['id_scr'], new_graph_df['id_dst']]))

    D.add_nodes_from(nodes_uniq)
    relationship=[tuple(map(int,re.findall('[0-9]+',x))) for x in new_graph_df_with_rl['relation']]
    D.add_edges_from(relationship)

    # Calculate pagerank of the nodes
    pr = nx.pagerank(D, alpha=alpha)

    pr_score = pd.DataFrame.from_dict(pr, orient='index').reset_index()
    pr_score.columns=['id', 'pr_score']

    pr_df=pd.merge(new_graph_df,pr_score, left_on=['id_scr'], right_on=['id'], how='left')
    pr_df.rename(columns = {'pr_score':'pr_score_scr'},inplace=True)
    pr_df=pd.merge(pr_df,pr_score,left_on=['id_dst'], right_on=['id'], how='left')
    pr_df.rename(columns = {'pr_score':'pr_score_dst'},inplace=True)
    pr_df=pr_df[['relation','follow','pr_score_scr','pr_score_dst']]

    if save_file:
        save_name = os.path.splitext(os.path.basename(filepath))[0] + '_withPRscore' + '.csv'
        save_file = os.path.dirname(os.path.realpath(filepath))
        save_name = os.path.join(save_file, save_name)
        pr_df.to_csv(save_name)
    print('Save pr_score file in url: '+ save_name)
    return D,pr
