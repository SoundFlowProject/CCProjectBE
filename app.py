
"""
Routes and views for the flask application.
"""

from datetime import datetime
from flask import render_template
from FlaskWebProject2 import app
import boto3, botocore
import pandas as pd
import json


@app.route('/')
@app.route('/home')
def home():
    """Renders the home page."""
    bucket_name = 'soundflow-songs'
    object_name_history = 'history.csv'
    object_name_data = 'data.csv'
    aws_access_key = 'AKIAZ3EZAPNJIHQH5HYC'
    aws_secret_key = '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'

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    df_history = download_from_s3(s3, bucket_name, object_name_history)
    df_data = download_from_s3(s3, bucket_name, object_name_data)

    join_df = df_history.merge(df_data, left_on='songId', right_on='id', how='inner').rename(columns={'name': 'Title', 'artists': 'Artist(s)'})[['Date', 'Text', 'Title', 'Artist(s)']]

    return render_template(
        'index.html',
        title='Home Page',
        df=join_df,
        year=datetime.now().year,
    )


def download_from_s3(s3, bucket_name, object_name):
    
    try:

        # Retrieve the contents of the S3 object
        response = s3.get_object(Bucket=bucket_name, Key=object_name)
    
        return pd.read_csv(response.get('Body')) # 'Body' is a key word

    except Exception as e:  
        print('ERROR: ', e)

app = Flask(__name__)

if __name__ == '__main__':
    app.run(port=8080)


