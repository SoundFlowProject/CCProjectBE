"""
Routes and views for the flask application.
"""

import boto3
import pandas as pd
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

"""Renders the home page."""
bucket_name = 'soundflow-songs-bucket'
object_name_history = 'history.csv'
object_name_data = 'data.csv'

s3 = boto3.client('s3')


def download_from_s3(s3, bucket_name, object_name):
    try:

        # Retrieve the contents of the S3 object
        response = s3.get_object(Bucket=bucket_name, Key=object_name)

        return pd.read_csv(response.get('Body'))  # 'Body' is a key word

    except Exception as e:
        print('ERROR: ', e)


df_history = download_from_s3(s3, bucket_name, object_name_history)
df_data = download_from_s3(s3, bucket_name, object_name_data)

join_df = df_history.merge(df_data, left_on='songId', right_on='id', how='inner')
filtered_df = join_df
# if id is not None:
#     filtered_df = join_df.where(join_df["userId"] == id)
final_df = filtered_df.rename(columns={'name': 'Title', 'artists': 'Artist(s)'})[
    ['Date', 'Text', 'Title', 'Artist(s)']]


@app.route('/')
def default():
    return 'helloo'


@app.route('/home', methods=['GET'])
def home(id=None):
    # Convert the DataFrame to JSON
    df_json = final_df.to_json(orient='records')

    # Create a Flask response with JSON content type
    response = jsonify(df_json)
    response.headers.add('Content-Type', 'application/json')
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route('/home', methods=['OPTIONS'])
def options():
    return '', 204, {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*'}


if __name__ == '__main__':
    app.run(port=8080)
