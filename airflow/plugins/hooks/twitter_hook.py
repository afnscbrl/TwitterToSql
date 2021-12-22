from airflow.hooks.http_hook import HttpHook
import requests
import json

#Creating a hook to connect the Airflow to Twitter

class TwitterHook(HttpHook):
    
    def __init__(self, query, conn_id = None):
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        super().__init__(http_conn_id=self.conn_id)
    
#This method create a url with the fields that i want get like twitter text and username
    def create_url(self):
        query = self.query
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,public_metrics"
        url = "{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}"
    
        return url

#This method connect us in TwitterAPI
    def connect_to_endpoint(self, url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {}).json()
 
#Cause our request create more than one page, this method verify if exist a another page 
#   next_token to open
    def paginate(self, url, session, next_token=""):
        if next_token:
            full_url = f"{url}&next_token={next_token}"
        else:
            full_url = url
        data = self.connect_to_endpoint(full_url, session)
        yield data
        
        if "next_token" in data.get("meta", {}):
            yield from self.paginate(url, session, data['meta']['next_token'])
        
    def run(self):
        session = self.get_conn()
        url = self.create_url()
        yield from self.paginate(url, session)

if __name__ == "__main__":
    for pg in TwitterHook("business").run():
        print(json.dumps(pg, indent=4, sort_keys=True))