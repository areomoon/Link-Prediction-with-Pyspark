import os
import numpy as np
import pandas as pd

def get_balance_data(filepath,number_to_get=400000):
    with open (filepath) as f:
        graph =[]
        for line in f.readlines():
            graph.append(line.strip('\n'))

    graph_df =pd.DataFrame(pd.Series(graph).str.split(' ' ,expand=True))
    columns_name = ["id_scr", "id_dst", "time_stamp"]
    graph_df.columns =columns_name
    graph_df['relation' ] =graph_df[['id_scr' ,'id_dst']].apply(tuple, axis=1)
    graph_df['follow' ] =1

    count_sample =number_to_get
    non_relation =[]
    while count_sample >0:
        new_set =set(zip(np.array(graph_df['id_dst']) ,np.random.permutation(graph_df['id_scr'],seed=0))).copy()
        new_set =new_set - set(graph_df['relation'])
        if len(new_set ) >count_sample:
            new_set =list(new_set)[:count_sample]
            non_relation +=(new_set)
            break
        else:
            count_sample -=len(new_set)
            non_relation +=list(new_set)
            continue

    non_relation_df =pd.concat([pd.Series(list(zip(*non_relation))[0]) ,pd.Series(list(zip(*non_relation))[1])] ,axis=1)
    non_relation_df.columns =['id_scr' ,'id_dst']
    non_relation_df['follow' ] =0
    non_relation_df['relation' ] =non_relation

    graph_df.drop('time_stamp',inplace=True ,axis=1)

    new_graph_df =pd.concat([graph_df ,non_relation_df])

    save_name = os.path.splitext(os.path.basename(filepath))[0] + '_balanced' + '.csv'
    save_file = os.path.dirname(os.path.realpath(filepath))
    save_name = os.path.join(save_file, save_name)
    new_graph_df.to_csv(save_name)
    return save_name
