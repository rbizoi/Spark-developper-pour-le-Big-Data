#! /usr/bin/python

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json

from kafka import KafkaProducer

access_token=""
access_token_secret=""
consumer_key=""
consumer_secret=""

producer = KafkaProducer(bootstrap_servers="localhost:9092")

class StdOutListener(StreamListener):

    def on_data(self, tweet):
        messsage = json.loads(tweet)['text']
        producer.send("tweets-flux", messsage.encode())
        return True

    def on_error(self, status):
        print (status)

if __name__ == '__main__':
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=['spark','machine learning',
                        'deep learning','machine learning',
                        'intelligence artificielle'],
                  languages=['en','fr'])
