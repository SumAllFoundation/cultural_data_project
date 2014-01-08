
from datetime import datetime, timedelta
from pandas import DataFrame, concat, Period, read_csv
from dateutil import parser
import pymongo 
from pandas import Period, DataFrame
import calendar
import pymongo
import sys, getopt
import json
import time


def twitter_daily_aggregate(retrievaldate,keys):

	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]

	mentions = dbtm.mentions
	user = dbtm.twitter_user
	tweets = dbtm.tweets
	#Date Retrieval
	d=[]
	dt = parser.parse(retrievaldate) + timedelta(days=-1)
	d.append(dt)
	d.append(d[-1] + timedelta(days=1))

	print 'start pulling data ....'

	try:
		while d[-1] < datetime.utcnow(): 			
			print 'processing ', d[-1], ' ..........'
			# Daily Favorite Count
			favorites = twitter_trend('favorite',tweets, d)
			#Daily Retweet Count
			retweets = twitter_trend('retweet',tweets, d)
			#Daily Mention Count
			mts = twitter_count(keys, d, 'mentions')
			#Daily Tweet Count
			twts = twitter_count(keys, d, 'tweets')
			#User Follower Count
			usrs =  twitter_follower(d,user)
			#Join and Add Date
			trend = favorites.join(retweets).join(mts).join(usrs).join(twts)
			trend['Date'] = Period(d[-1],'D')
			#Start DataFrame
			if len(d) == 2:
				ctrend = trend.copy()
				# fol = followers.copy()
			else:
				ctrend = concat([ctrend,trend])
			#Extend Dates
			d.append(d[-1] + timedelta(days=1))
	except:
			print 'pass', d[-1]
	#Join DataFrames and Fill NAs
	ctrend['mentions'] = ctrend['mentions'].fillna(0)
	ctrend['tweets'] = ctrend['tweets'].fillna(0)
	#To CSV
	print 'saving the csv file'
	ctrend.to_csv('trend{0}.csv'.format(retrievaldate))
	return ctrend

def twitter_follower(d,db):

	#upper_bound_end_ts = float(calendar.timegm(d[-1].utctimetuple())*1000); upper_bound_start = d[-1] - timedelta(days=1); upper_bound_start_ts = float(calendar.timegm(upper_bound_start.utctimetuple())*1000)		
	#The Day Of
	upper_bound_start_ts = float(calendar.timegm(d[-1].utctimetuple())*1000); 
	upper_bound_end = d[-1] + timedelta(days=1); 
	upper_bound_end_ts = float(calendar.timegm(upper_bound_end.utctimetuple())*1000)
	#MongoDB 
	users = db.aggregate([{'$match':{'timestamp':{"$gt": upper_bound_start_ts, "$lt": upper_bound_end_ts}}},
		                {'$project':{'cdpid':1,'screen_name':1,'followers_count':1,'friends_count':1}},
		                #Average by Screenname if the same user name is retrieved twice
		                {'$group': {'_id': '$screen_name', 'friends_count': {'$avg':'$friends_count'},'followers_count': {'$avg':'$followers_count'},'cdpid':{'$addToSet':'$cdpid'}}},
		                {'$unwind':'$cdpid'},
		                {'$group': {'_id': '$cdpid', 'friends': {'$sum':'$friends_count'},'followers': {'$sum':'$followers_count'}}}])
	#Followers
	users = DataFrame(users['result'])
	users.index = users._id;  users=users.drop('_id',axis=1); users = users.sort_index();
	print 'Friends and Followers for ', d[-1], ' processed'
	#users['Date'] = Period(d[-2],'D')
	return(users)

def twitter_count(keys,d,strdb):

	#Mongo
	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]
	db = dbtm[strdb]
	#MongoDB Query - Mentions
	#The Day Of
	upper_bound_start_ts = float(calendar.timegm(d[-1].utctimetuple())*1000); 
	upper_bound_end = d[-1] + timedelta(days=1); 
	upper_bound_end_ts = float(calendar.timegm(upper_bound_end.utctimetuple())*1000)
	#upper_bound_end_ts = float(calendar.timegm(d[-1].utctimetuple())*1000); upper_bound_start = d[-1] - timedelta(days=1); upper_bound_start_ts = float(calendar.timegm(upper_bound_start.utctimetuple())*1000)
	# #Retrieve Tweeets that are not authored by the user itself. 
	if strdb in 'mentions':
		tr = 	db.aggregate([
								{'$match': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}},
								{'$unwind':'$cdpid'},
								{'$group':{'_id':'$cdpid',strdb:{'$sum':1}}}])
	#Tweets collection does not need unwind unlike mentions collection. 
	else:
		tr = 	db.aggregate([
							{'$match': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}},
							{'$group':{'_id':'$cdpid',strdb:{'$sum':1}}}])
	tr = DataFrame(tr['result']); 
	tr.index = tr._id;  tr=tr.drop('_id',axis=1); tr = tr.sort_index();
	#mts['Date'] = Period(d[-2],'D')
	print '%s for ' %(strdb), d[-1], ' processed'
	return(tr)

	###

	#tr = 	db.aggregate([{'$match': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}}])

def twitter_trend(trendname,db,d):

	##Date Retrieval
	# d=[]
	# d.append(parser.parse(date))
	# d.append(d[-1] + timedelta(days=1))
	#The Day Of
	upper_bound_start_ts = float(calendar.timegm(d[-1].utctimetuple())*1000); 
	upper_bound_end = d[-1] + timedelta(days=1); 
	upper_bound_end_ts = float(calendar.timegm(upper_bound_end.utctimetuple())*1000)
	#Previous Day
	lower_bound_start_ts = float(calendar.timegm(d[-2].utctimetuple())*1000);
	lower_bound_end = d[-2] + timedelta(days=1);
	lower_bound_end_ts = float(calendar.timegm(lower_bound_end.utctimetuple())*1000)
	# #Defining LookBack Period - First order approximation to get the user interactions on a daily basis.
	# #THIs ensures that no tweets fall off from the grid. 
	# lb = d[-1] - timedelta(days=lookback); lookback_timestamp = float(calendar.timegm(lb.utctimetuple())*1000)
	#TrendLine Selection+
	#open('%s.json'% (keyname)) as f:
	if trendname in ['retweet','retweets']:
		key = 'retweet_count'
	elif trendname in ['favorite','favorites']:
		key = 'favorite_count'	
	print key
	#MongoDB Aggregation
	#New, not retweeeted.
	new = db.aggregate([
					#Retrieve tweets created last two days. No retweets
					{'$match':{'timestamp':{'$gt': lower_bound_start_ts, '$lt':upper_bound_end_ts}, 'retweeted_status':{'$exists':False}}},
					#Match only the tweets with a timestamp on that particular day, not earlier.
					{'$match':{
								'stats': {'$all':[
											{'$elemMatch': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}},
											{'$elemMatch': {'timestamp':{'$not':{'$lt': upper_bound_start_ts}}}}
												]
										}
								}
					},
					{'$unwind':'$stats'},
					#Filter Last Time Stamp
					{'$match':{'stats.timestamp':{"$gt": upper_bound_start_ts, "$lt": upper_bound_end_ts}}},
					{'$project':{'cdpid':1, key: '$stats.%s'%(key)}},
	                #Group By tweet and take average if there are more than one timestamp on
	                {'$group': {'_id': '$_id', key: {'$avg':'$%s'%(key)},'cdpid':{'$addToSet':'$cdpid'}}},
	                {'$unwind':'$cdpid'},
	                #Group by ID
					{'$group':{'_id':'$cdpid','new':{'$sum':'$%s'%(key)}}}])
	#Existing
	first= db.aggregate([
					#Match Tweets that have a timestamp for consecutive days.
					#http://docs.mongodb.org/manual/reference/operator/query/all/
					{'$match':{
					'stats': {'$all':[
										{'$elemMatch': {'timestamp':{'$gt': lower_bound_start_ts, '$lt': lower_bound_end_ts}}},
										{'$elemMatch': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}}
									]
							}
					}},
					#Retweet False
					{'$match':{'retweeted_status':{'$exists':False}}},
					#Unwind
					{'$unwind':'$stats'},
					#Find the lower bound tweets
					{'$match':{'stats.timestamp':{"$gt": lower_bound_start_ts, "$lt": lower_bound_end_ts}}},
	                {'$project':{'cdpid':1, key: '$stats.%s'%(key)}},
	                #Group By tweet and take average if there are more than one timestamp on
	                {'$group': {'_id': '$_id', key: {'$avg':'$%s'%(key)},'cdpid':{'$addToSet':'$cdpid'}}},
	                {'$unwind':'$cdpid'},
	                #Finally, Group By ID. 
	                {'$group':{'_id':'$cdpid','first':{'$sum':'$%s'%(key)}}}])

	second= db.aggregate([
					#Match Tweets that have a timestamp for consecutive days.
					#{'$match':{'stats.timestamp':{{"$gt": lower_bound_start_ts, "$lt": lower_bound_end_ts},{"$gt": upper_bound_start_ts, "$lt": upper_bound_end_ts}}}},						
					{'$match':{
					'stats': {'$all':[
										{'$elemMatch': {'timestamp':{'$gt': lower_bound_start_ts, '$lt': lower_bound_end_ts}}},
										{'$elemMatch': {'timestamp':{'$gt': upper_bound_start_ts, '$lt': upper_bound_end_ts}}}
									]
							}
					}},
					#Retweet False
					{'$match':{'retweeted_status':{'$exists':False}}},
					#Unwind
					{'$unwind':'$stats'},
					#Find the upper bound tweets
					{'$match':{'stats.timestamp':{"$gt": upper_bound_start_ts, "$lt": upper_bound_end_ts}}},
	                {'$project':{'cdpid':1, key: '$stats.%s'%(key)}},
	                #Group By tweet and take average if there are more than one timestamp on
	                {'$group': {'_id': '$_id', key: {'$avg':'$%s'%(key)},'cdpid':{'$addToSet':'$cdpid'}}},
	                {'$unwind':'$cdpid'},
	                #Finally, Group By ID. 
	                {'$group':{'_id':'$cdpid','second':{'$sum': '$%s'%(key)}}}])
	#DataFrame Conversion
	#New
	new = DataFrame(new['result']); 
	new.index = new._id;  new=new.drop('_id',axis=1); new = new.sort_index();	
	# new2 = DataFrame(new2['result']); 
	# new2.index = new2._id;  new2=new2.drop('_id',axis=1); new2 = new2.sort_index();	
	#Previous Day
	first = DataFrame(first['result']); 
	first.index = first._id;  first=first.drop('_id',axis=1); first = first.sort_index();
	#Current Day		
	second = DataFrame(second['result']); 
	second.index = second._id;  second=second.drop('_id',axis=1); second = second.sort_index();
	#Merge New Tweets with Existing Tweets
	#Join and Create a temp table. Join inner to match keys - which should match in terms of length.
	temp = second.join(new,how='outer').join(first,how='inner'); temp = temp.fillna(0)
	trend = DataFrame(temp['new'] + temp['second'] - temp['first']);
	trend.columns = [trendname]
	# trend['Date'] = Period(d[-2],'D')
	print 'Total ', key, 'count for new Tweets for ', d[-1], ' : ', int(temp.new.sum())
	print 'Total ', key, 'count for existing Tweets for ',d[-1], ' : ', int(temp.second.sum() - temp['first'].sum())
	return trend

if __name__ == '__main__':

	try:
		opts,args = getopt.getopt(sys.argv[1:],'k:s:', ['keys=','start='])
	except getopt.GetoptError:
		print 'Exit'
	#Parse Arguments
	for opt, arg in opts:
		if opt in ('-k','--keys'):
			keyname = arg

		elif opt in ('-s','--start'):
			startdate = arg

	#Read Input File
	print 'Starting retrieving daily updates'
	print datetime.now()

	try:
		#Keys
		keys=[]
		with open('%s.json'% (keyname)) as f: 
			for line in f: keys.append(json.loads(line))
		keys = keys[0]
		twitter_daily_aggregate(startdate,keys)
	except:
		print 'Key file is not found. Exiting now....'
		sys.exit(0)

	


