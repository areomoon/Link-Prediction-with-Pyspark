
import pandas as pd
import re
import os
import time

# format
class tweet_extractor():
    '''
    The object is used to extract tweet url and content data from "tweet_without_word" directory
    '''
    def __init__(self,file_dir,cont_save_name='tweet_content_files',url_save_name='tweet_url_files'):
        '''

        :param file_dir: filepath ex:tweet_without_word/2011 or /2010
        :param cont_save_name: defaulted save file name
        :param url_save_name:  defaulted save file name
        '''
        self.file_dir=file_dir
        self.cont_save_name=cont_save_name
        self.url_save_name=url_save_name

    def get_tweet_content_files(self):
        files=self.get_txt_file_name(self.file_dir)
        print('Start to extract tweet content as csv: '+str(files))
        if not os.path.exists(self.cont_save_name):
            os.makedirs(self.cont_save_name)
        acc_t=0
        for txt_file in files:
            start_t = time.time()
            self.extract_content_as_csv(os.path.join(self.file_dir,txt_file),self.cont_save_name)
            end_t = time.time()
            take_t=end_t-start_t
            acc_t+=take_t
            print(txt_file+' takes {:.2f} seconds to be processed'.format(take_t))
        print('===============================================')
        print('Finish tweet content extractor work. Total time {:.2f}'.format(acc_t))

    def get_tweet_url_files(self):
        files=self.get_txt_file_name(self.file_dir)
        print('Start to extract tag_url as csv: '+str(files))
        if not os.path.exists(self.url_save_name):
            os.makedirs(self.url_save_name)
        acc_t=0
        for txt_file in files:
            start_t = time.time()
            self.extract_tag_url_as_csv(os.path.join(self.file_dir,txt_file),self.url_save_name)
            end_t = time.time()
            take_t=end_t-start_t
            acc_t+=take_t
            print(txt_file+' takes {:.2f} seconds to be processed'.format(take_t))
        print('===============================================')
        print('Finish tweet tag_url extractor work. Total time {:.2f}'.format(acc_t))

    def get_txt_file_name(self,dir_path):
        return [file for file in os.listdir(dir_path) if file.endswith(".txt")]

    def extract_content_as_csv(self,file_path,save_file):
        f=open(file_path,'r',encoding='latin1').read()
        tw=re.split(r'\n+\n',f)
        df=pd.DataFrame([lines.strip('\n') for lines in tw])
        mask=(df[0].str.len()>1)
        df=df.loc[mask]
        df[0]=df[0].apply(lambda x :re.split('\n',x))

        df['user']=df[0].apply(lambda x: x[0])
        df['tweet_content']=df[0].apply(lambda x: re.split('\s',x[6]) if len(x)>7 else None)
        df_subset=df.dropna(subset=['tweet_content'])
        df_with_tw_content=df_subset.groupby('user')['tweet_content'].apply(lambda x : list(set([subwords for words in x for subwords in words])))
        save_name= os.path.splitext(os.path.basename(file_path))[0]+'cont'+'.csv'
        save_name=os.path.join(save_file,save_name)
        df_with_tw_content.to_csv(save_name)

    def extract_tag_url_as_csv(self,file_path,save_file):
        f=open(file_path,'r',encoding='latin1').read()
        tw=re.split(r'\n+\n',f)
        df=pd.DataFrame([lines.strip('\n') for lines in tw])
        mask=(df[0].str.len()>1)
        df=df.loc[mask]
        df[0] = df[0].apply(lambda x: re.split('\n', x))

        df['user'] = df[0].apply(lambda x: x[0])
        df['tag_url'] = df[0].apply(lambda x: [url[10:] for url in x[8:]] if len(x) > 9 else None)
        df_subset = df.dropna(subset=['tag_url'])
        df_with_url=df_subset.groupby('user')['tag_url'].apply(lambda x : list(set([subwords for words in x for subwords in words])))
        save_name= os.path.splitext(os.path.basename(file_path))[0]+'url'+'.csv'
        save_name=os.path.join(save_file,save_name)
        df_with_url.to_csv(save_name)

def get_name_n_hashtag(filepath,header=None):
    s = pd.read_csv(filepath,header=header)
    s['username'] = [re.findall(r"((?<=\'username\\t/).*?\')", i) for i in s[1]]
    s['hashtag'] = [re.findall(r"((?<=\'hashtag\\t/search\?q\=\%23).*?\')", i) for i in s[1]]
    s['username'] = s['username'].apply(lambda x: re.sub('\"', " ", re.sub("\[|\]|\'|,", " ", str(x))))
    s['hashtag'] = s['hashtag'].apply(lambda x: re.sub('\"', " ", re.sub("\[|\]|\'|,", " ", str(x))))
    s.drop([1], axis=1, inplace=True)
    s.rename(columns={0: 'id'}, inplace=True)
    save_name = os.path.splitext(os.path.basename(filepath))[0] + '_tag_n_username' + '.csv'
    save_file = os.path.dirname(os.path.realpath(filepath))
    save_name = os.path.join(save_file, save_name)
    s.to_csv(save_name)