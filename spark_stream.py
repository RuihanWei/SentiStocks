
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob
# import re

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('time', F.regexp_extract('word', r'<time_encoding_sentistock_101>(.*?)<\/time_encoding_sentistock_101>', 1))
    words = words.withColumn('word', F.regexp_replace('word', '<time_encoding_sentistock_101>', ''))
    words = words.withColumn('word', F.regexp_replace('word', r'<\/time_encoding_sentistock_101>', ''))
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    # words = add_time(words, lines)
    return words

    # whitespace = re.compile(r"\s+")
    # web_address = re.compile(r"(?i)http(s):\/\/[a-z0-9.~_\-\/]+")
    # tesla = re.compile(r"(?i)@Tesla(?=\b)")
    # user = re.compile(r"(?i)@[a-z0-9_]+")

    # tweet = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    # # we then use the sub method to replace anything matching
    # tweet = whitespace.sub(' ', tweet)
    # tweet = web_address.sub('', tweet)
    # tweet = tesla.sub('Tesla', tweet)
    # tweet = user.sub('', tweet)
    # return tweet

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words
    
# def add_time(words, lines):
#     words.withColumn("time", lines)

if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    # read the tweet data from socket
    lines = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 3333).load() #127.0.0.1
    # Preprocess the data

    # tweet_date = lines.split("<time-endcode>")[1]
    # lines = lines.split("<time-endcode>")[0]
    
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    # words = add_time(words, tweet_date)

    words = text_classification(words)
    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets")\
        .format("csv")\
        .option("format", "append")\
        .option("path", "./res")\
        .option("checkpointLocation", "./res/check")\
        .trigger(processingTime='200 seconds')\
        .start()
    query.awaitTermination()



        # .option("path", "C:\\Users\\Tobyw\OneDrive - University of Waterloo\\3B.5\\projects")\


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.functions import col, split

# if __name__ == "__main__":

#     # create Spark session
#     spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()

#     # read the tweet data from socket
#     tweet_df = spark \
#         .readStream \
#         .format("socket") \
#         .option("host", "127.0.0.1") \
#         .option("port", 3333) \
#         .load()

#     # type cast the column value
#     tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")


#     # split words based on space, filter out hashtag values and group them up
#     tweets_tab = tweet_df_string.withColumn('word', explode(split(col('value'), ' '))) \
#         .groupBy('word') \
#         .count() \
#         .sort('count', ascending=False). \
#         filter(col('word').contains('#'))

#     # write the above data into memory. consider the entire analysis in all iteration (output mode = complete). and let the trigger runs in every 2 secs.
#     writeTweet = tweets_tab.writeStream. \
#         outputMode("complete"). \
#         format("memory"). \
#         queryName("tweetquery"). \
#         trigger(processingTime='2 seconds'). \
#         start()

#     print("----- streaming is running -------")