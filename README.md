## Tweets to Postgresql with airflow and Spark

**Intro:**
  I developed a system that get tweets in Twitter API and transform it on data lake to push them on database with Postgresql.

**Motivation and Goal:**
  I was tryin understand how Airflow works to orchestrate tasks and how Spark manipulate data, basicaly i was studyin a real case of ETL and Datalake with this two tools.
  
**Phases:**
  - Testing Twitter API
  - Connecting to Twitter
  - Creating a DAG
  - Getting the data
  - First tranformation of the data (Bronze to Silver)
  - Second transformation of the data (Silver to Gold)
  - Putting the transformations in the Airflow dag as task
  - Filling in a table database with the tweets of the Gold stage with a task in Airflow.

### Testing the Twitter API

First of all, i developed a small script to test the Twitter API and look at the data response, in this case, a json file. This script i dont included in final repo cause its made for test but a quote it here:

```
import requests
import os
import json

def auth():
    return os.environ.get("BEARER_TOKEN")

def create_url():
    query = "AluraOnline"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
    user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
    filters = "start_time=2021-02-15T00:00:00.00Z&end_time=2021-02-19T00:00:00.00Z"
    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query, tweet_fields, user_fields, filters
    )
    return url

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

def paginate(url, headers, next_token=""):
    if next_token:
        full_url = f"{url}&next_token={next_token}"
    else:
        full_url = url
    data = connect_to_endpoint(full_url, headers)
    yield data
    if "next_token" in data.get("meta", {}):
        yield from paginate(url, headers, data['meta']['next_token'])

def main():
    bearer_token = auth()
    url = create_url()
    headers = create_headers(bearer_token)
    for json_response in paginate(url, headers):
        print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
```
With the json file in hand, i could understand better how to manipulate the data.

### Connecting to Twitter

