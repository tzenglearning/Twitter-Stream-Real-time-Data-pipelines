#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 28 21:10:47 2019

@author: tzlearning
"""

import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours Access Token
ACCESS_TOKEN = "1178113359448813568-gnf6WQiHOAJUx4dVE7XC1IDmp40iYZ"
ACCESS_SECRET = "L5m1TqRM6cXhd6iaALd7JVQEZqaBa4HLKp5mI0RjIyaTQ"
CONSUMER_KEY = "zyGOlLDusbggBDDbGiWLuvg9N"
CONSUMER_SECRET = "SHL6oz9p7UlRMCtYE0XDHS5IgVmjShto386wggNaLpARXn3ese"
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)

def getTweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # query_data = [('language', 'en'), ('locations', '-122,-37,-122,38'),('track','#')]
    query_data = [('language', 'en'), ('locations', '-90,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text'] + '\n'      # pyspark can't accept stream, add '\n'

            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text.encode('utf-8', errors='ignore'))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
TCP_IP = "localhost"
TCP_PORT = 9090
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected. getting tweets.")
resp = getTweets()
send_tweets_to_spark(resp,conn)