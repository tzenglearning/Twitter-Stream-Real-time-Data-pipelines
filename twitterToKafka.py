from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler


access_token = "hidden"
access_token_secret = "hidden"
consumer_key = "hidden"
consumer_secret = "hidden"

class KafkaListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("twitterStream", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = KafkaListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="twitterStream")
