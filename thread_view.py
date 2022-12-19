import os
import requests
import pandas as pd
from twarc import Twarc2
from twarc_csv import DataFrameConverter
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
from dotenv import load_dotenv

from rich.tree import Tree

from utils import get_cluster_info


load_dotenv()

st.set_page_config(layout="wide")

@st.cache
def get_conversation(conversation_id):
    convo_tweets = []
    for page in twarc.search_recent(f"conversation_id:{conversation_id}", max_results=100):
        page_df = converter.process([page])
        convo_tweets.append(page_df)
    return pd.concat(convo_tweets)

def add_next_level(current_tweet, df, tree):
    reply_df = df[df["referenced_tweets.replied_to.id"] == current_tweet.id]
    if reply_df.shape[0] > 0:
        for index, row in reply_df.iterrows():
            reply_tree = tree.add(f"{row['author.username']}, {row.id}: {row.text} \n\n")
            return add_next_level(row, df, reply_tree)
    else:
        return tree

def build_convo_graph(convo_df):
    print("building convo graph")
    print(convo_df.shape)
    conversation_id = convo_df.iloc[0]["conversation_id"]
    convo_tweet = convo_df[convo_df["id"] == conversation_id]
    if convo_tweet.shape[0] == 0:
        print(f"conversation {conversation_id} is missing convo tweet... using oldest tweet as replacement")
        convo_tweet = convo_df.sort_values("created_at").iloc[0]
        print(convo_tweet)
    else: 
        convo_tweet = convo_tweet.iloc[0]
    
    tree = Tree(f"[green]{convo_tweet['author.username']}, {convo_tweet.id}: {convo_tweet.text} \n\n")
    
    reply_df = convo_df[convo_df["referenced_tweets.replied_to.id"] == convo_tweet.id]
    
    for index, current_tweet in reply_df.iterrows():
        reply_tree = tree.add(f"{current_tweet['author.username']}, {current_tweet.id}: {current_tweet.text} \n\n")
        add_next_level(current_tweet, convo_df, reply_tree)
    return tree

twarc = Twarc2(bearer_token=os.environ["TWITTER_TOKEN"])
converter = DataFrameConverter(input_data_type="tweets", allow_duplicates=True)   

st.title("Community Thread View")

# streamlit text input for a tweet url or a tweet id, if it is a url, it parses the id from the url
tweet_input_id = st.text_input("Enter any Tweet URL or ID", "1603765939723534337")

params = st.experimental_get_query_params()

if "tweet_id" in params:
    params_tweet_id = params["tweet_id"][0]
else:
    params_tweet_id = None

if tweet_input_id != params_tweet_id:
    tweet_id = tweet_input_id
    st.experimental_set_query_params(tweet_id=tweet_id)
else: 
    tweet_id = params_tweet_id

# tweet_id = st.text_input("Tweet URL or ID","https://twitter.com/coladaclan/status/1603765939723534337")

# parse the tweet id from the url
if "twitter.com" in tweet_id:
    tweet_id = tweet_id.split("/")[-1]

# lookup the tweet from twarc and parse out the conversation id

initial_tweets = []
for page in twarc.tweet_lookup([tweet_id]):
    page_df = converter.process([page])
    initial_tweets.append(page_df)

tweet_id = initial_tweets[0]["id"].iloc[0]
conversation_id = initial_tweets[0]["conversation_id"].iloc[0]
if tweet_id != conversation_id:
    for page in twarc.tweet_lookup([conversation_id]):
        page_df = converter.process([page])
        initial_tweets.append(page_df)

convo_df = pd.concat(initial_tweets)

st.write("conversation id:", conversation_id)

import requests
import streamlit.components.v1 as components


class Tweet(object):
    def __init__(self, s, embed_str=False):
        if not embed_str:
            # Use Twitter's oEmbed API
            # https://dev.twitter.com/web/embedded-tweets
            api = "https://publish.twitter.com/oembed?url={}".format(s)
            response = requests.get(api)
            self.text = response.json()["html"]
        else:
            self.text = s

    def _repr_html_(self):
        return self.text

    def component(self):
        return components.html(self.text, height=400, scrolling=True)

st.subheader("The starter tweet (the tweet where tweet_id == conversation_id)")
t = Tweet(f"https://twitter.com/OReillyMedia/status/{conversation_id}").component()

st.subheader("Reply Summary Stats")
convo_df =  pd.concat([convo_df, get_conversation(conversation_id)])
convo_df = convo_df.drop_duplicates("id")
st.write(f"**{convo_df.shape[0]} total tweets** in this thread.")
st.write(f"**{convo_df['author.username'].nunique()} unique accounts** posted in this thread.")
tree = build_convo_graph(convo_df)

author_cols = [i for i in convo_df.columns if i.startswith("author.")]
author_df = convo_df[author_cols].drop_duplicates()

@st.cache
def get_borg(author_df):
    return get_cluster_info(author_df, id_col="author.id", username_col="author.username")

borg_community_df = get_borg(author_df)

st.write(f"Users in this thread are part of **{borg_community_df['clusters.name'].nunique()} unique hive clusters.**")
st.write(f'{borg_community_df[borg_community_df["clusters.name"].isna()].shape[0]} users in this thread are not included in any clusters.')

# get a list of the clusters that each author.username is a part of in borg_community_df

num_tweets = convo_df.groupby("author.username").agg(
    num_tweets = pd.NamedAgg(column="author.username", aggfunc="count")
).sort_values("num_tweets", ascending=False)

user_clusters = borg_community_df.groupby("author.username")["clusters.name"].apply(list)
num_tweets["clusters"] = user_clusters

st.dataframe(num_tweets.reset_index(), use_container_width=True)

community_grouping = borg_community_df.groupby('clusters.name').agg({'author.username': 'count'}, dropna=False).reset_index().sort_values('author.username', ascending=False)

st.subheader("Community Distribution")
num_unique_communities = borg_community_df["clusters.name"].nunique()
starter_max = 25 if num_unique_communities > 25 else num_unique_communities
min_num_communities, max_num_communities = st.slider("use this slider to select the number of top communities to display", min_value=0, max_value=num_unique_communities, value=(0,starter_max), step=5)
fig = px.bar(
    community_grouping.iloc[min_num_communities:max_num_communities], 
    x="clusters.name", 
    y="author.username", 
    color="clusters.name", 
    title=f"Community distribution of accounts posting in thread"
)
st.plotly_chart(fig)




st.subheader("Conversation Tree")
from rich.console import Console
console = Console(record=True)
with console.capture() as capture:
    console.print(tree)
str_o = console.export_html()

components.html(str_o, scrolling=True, height=500, width=1500)
