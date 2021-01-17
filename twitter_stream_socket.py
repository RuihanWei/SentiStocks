import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket 
import json
import time


# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object while passing in auth information
api = tweepy.API(auth) 

# ts = time.strftime('%Y-%m-%d-%H', time.strptime(msg['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))

class TweetsListener(StreamListener):
  # tweet object listens for the tweets
  def __init__(self, csocket):
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      print("new message")
      # if tweet is longer than 140 characters
      ts = time.strftime('%Y-%m-%d-%H', time.strptime(msg['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
      time_msg = "<time_encoding_sentistock_101>" + ts + "</time_encoding_sentistock_101>"
      
      if "extended_tweet" in msg:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['extended_tweet']['full_text'] + time_msg +"t_end")\
            .encode('utf-8'))         
        print(msg['extended_tweet']['full_text'])
      else:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['text']+ time_msg +"t_end")\
            .encode('utf-8'))
        print(msg['text'])
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True

# class TweetsListener(StreamListener):
#     # initialized the constructor
#     def __init__(self, csocket):
#         self.client_socket = csocket

#     def on_data(self, data):
#         try:
#             # read the Twitter data which comes as a JSON format
#             msg = json.loads(data)
            
            
#             # the 'text' in the JSON file contains the actual tweet.
#             print(msg['text'].encode('utf-8'))

#             # the actual tweet data is sent to the client socket
#             # self.client_socket.send(msg['text'].encode('utf-8'))
#             self.client_socket.send((msg['text']+"<time-endcode>"+ts).encode('utf-8'))
#             return True

#         except BaseException as e:
#             # Error handling
#             print("Ahh! Look what is wrong : %s" % str(e))
#             return True

#     def on_error(self, status):
#         print(status)
#         return True


def sendData(c_socket):
    # authentication
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # twitter_stream will get the actual live tweet data
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    # filter the tweet feeds related to "corona"
    twitter_stream.filter(track=['Facebook'], languages=["en"])
    # in case you want to pass multiple criteria
    # twitter_stream.filter(track=['DataScience','python','Iot'])


# create a socket object
s = socket.socket()

# Get local machine name : host and port
host = "127.0.0.1"
port = 3333

# Bind to the port
s.bind((host, port))
print("Listening on port: %s" % str(port))

# Wait and Establish the connection with client.
s.listen(5)
c, addr = s.accept()

print("Received request from: " + str(addr))

# Keep the stream data available
sendData(c)