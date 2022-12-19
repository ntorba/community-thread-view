import os
import concurrent.futures
import requests
import pandas as pd
from twarc import Twarc2
from twarc_csv import DataFrameConverter
import streamlit as st
import click
from dotenv import load_dotenv

load_dotenv()

twarc = Twarc2(bearer_token=os.environ["TWITTER_TOKEN"])
converter = DataFrameConverter(input_data_type="users", allow_duplicates=False)   

BORG_API_ENDPOINT = "https://api.borg.id/influence/influencers/twitter:{}/"


def get_borg_influence(user,id_col="id"):
    # make borg request with BORG_API_KEY env var included in request headers 
    response = requests.get(BORG_API_ENDPOINT.format(user[id_col]), headers={"Authorization": f'Token {os.environ["BORG_API_KEY"]}'})
    return user, response.json()


def get_cluster_info(df, id_col="id", username_col="username"):
    df_rows = []
    # use conncurrent.futures to make requests to the borg api in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(get_borg_influence, user, id_col=id_col) for user in df.to_dict(orient="records")]
        for future in concurrent.futures.as_completed(futures):
            user, borg_influence = future.result()
            if 'error' in borg_influence:
                print(f"user {user[username_col]} is not indexed by borg")
                df_rows.append(user)
                continue
            clusters = borg_influence.get("clusters", [])
            if len(clusters) < 1:
                # get the row from df where id == user_id as a dict
                df_rows.append(user)
                continue

            clusters_by_id = {cluster["id"]: cluster for cluster in clusters}

            latest_scores = borg_influence["latest_scores"]

            for score_dict in latest_scores:
                cluster_id = score_dict["cluster_id"]
                score_dict = {f'latest_scores.{key}': value for key, value in score_dict.items()}
                cluster = clusters_by_id[cluster_id]
                cluster = {f'clusters.{key}': value for key, value in cluster.items()}
                row = {**score_dict, **cluster, **user}
                df_rows.append(row)
    return pd.DataFrame(df_rows)
