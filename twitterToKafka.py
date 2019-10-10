from kafka import SimpleProducer, KafkaClient
from tweepy.streaming import StreamListener
from tweepy import Stream
from tweepy import OAuthHandler


access_token = "1178113359448813568-gnf6WQiHOAJUx4dVE7XC1IDmp40iYZ"
access_token_secret = "L5m1TqRM6cXhd6iaALd7JVQEZqaBa4HLKp5mI0RjIyaTQ"
consumer_key = "zyGOlLDusbggBDDbGiWLuvg9N"
consumer_secret = "SHL6oz9p7UlRMCtYE0XDHS5IgVmjShto386wggNaLpARXn3ese"

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