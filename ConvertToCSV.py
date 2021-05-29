import json
import csv

# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

tweets = []
for line in open('project_tweets_file.json', 'r'):
    tweets.append(json.loads(line))

fileName = "solr_data.csv"
fields = ['created_at', 'id', 'text', 'source', 'user_name', 'user_location', 'user_followers_count',
          'user_friends_count', 'user_favourites_count', 'user_statuses_count', 'quote_count', 'reply_count',
          'retweet_count', 'favorite_count']

with open(fileName, 'wb') as newFile:
    csvwriter = csv.writer(newFile)

    # writing the fields
    csvwriter.writerow(fields)
    for tweet in tweets:
        row= [tweet['created_at'], tweet['id'], tweet['text'], tweet['source'], tweet['user']['name'],
          tweet['user']['location'], tweet['user']['followers_count'], tweet['user']['friends_count'],
          tweet['user']['favourites_count'], tweet['user']['statuses_count'], tweet['quote_count'],
          tweet['reply_count'], tweet['retweet_count'], tweet['favorite_count']]
        print row
        csvwriter.writerow(row)

