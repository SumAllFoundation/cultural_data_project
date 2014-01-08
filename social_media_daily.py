#!/usr/local/bin/python python

def facebook_all_retrieve(keys,df):
	#Retrieve Posts, pictures, videos and track the number of likes and comments count.
	#Only the first page retrieved to efficiently use 1 hour token.
	import facebook
	import pymongo
	import time
	import calendar
	from dateutil import parser
	from datetime import datetime
	import urllib2
	import json
	#Here we go!! 
	print 'Processing Facebook now ... '
	print datetime.now()
	#For Broken Links
	br = []
	#Filter
	df= df[df.social=='facebook']
	#Connect
	fb = facebook.GraphAPI(keys['facebook']['token'])
	#Database
	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]
	facebook_posts = dbtm.facebook_posts
	#facebook_post = dbtm.facebook_post

	facebook_user = dbtm.facebook_user
	#Broken Links
	broken = []
	#Collect
	for i,user in df.iterrows():
		print i
		try:
			#User Profile####################
			profile = fb.get_object(user['handle'])
			print profile['name'], user.name, ' profile retrieved'
			#Insert ID
			profile[u'cdpid'] = int(user.name)
			#Insert Date - As of Now
			profile[u'timestamp'] = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
			facebook_user.insert(profile)
			#Posts
			posts = fb.get_connections(user['handle'],"posts")
			print user.name
			#Not Paging currently. d
			for post in posts['data']:
				#Check if the entry in the database already
				query = facebook_posts.find({'_id':post['id']})
				#Facebook Stats
				stat = facebook_stats(post)
				#Insert. New Post.
				if query.count() == 0:
					post['_id'] = str(post['id'])
					post[u'cdpid'] = int(user.name)	
					#TimeStamp				
					dt = parser.parse(post['created_time'])
					post[u'timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
					#Start Array of Stats
					post[u'stats']=[stat]
					facebook_posts.insert(post)
					print post['_id'], 'for user ', user.name, ' inserted'
				else:
					#Update the document with most recent comment,likes and updated time.
					#Update the like count and comment count
					#Like count updated regardless
					#Comment count updated only if there is a new comment.
					post = query.next()
					print 'post ', post['id'], ' for user ', user.name, ' in the database. Updating the statistics..'
					#Push the stat ino the array
					facebook_posts.update({'_id':post['_id']},{'$push':{'stats':stat}})
					#Update comments, likes, shares, updated time
					if u'comments' in post.keys(): facebook_posts.update({'_id':post['_id']},{'$set':{'comments':post['comments']}}) 
					if u'likes' in post.keys(): facebook_posts.update({'_id':post['_id']},{'$set':{'likes':post['likes']}})
					if u'shares' in post.keys(): facebook_posts.update({'_id':post['_id']},{'$set':{'shares':post['shares']}})
					facebook_posts.update({'_id':post['_id']},{'$set':{'updated_time':post['updated_time']}})
					print 'Updated stats for ',post['_id'], '. Last comment date: ', post['updated_time'] 
		except Exception,e:
			print e
			try:
				err = e.result
				if err['error']['code']== 190:
					print user.name, ' break. good-bye!'
					return(broken)
					break
				else:
					print user['handle'], ' not retrieved'
					broken.append(user.name)
			except Exception,e:
				print e
	return(broken)


def facebook_stats(post):
	import calendar
	import json
	from datetime import datetime
	from dateutil import parser
	#Create an Array of Likes, Comments, Shares.
	#Likes
	like_count = 0
	if u'likes' in post.keys():
		cursor, like_count = post[u'likes'], len(post[u'likes'][u'data'])
		while u'next' in cursor.keys():
			next = cursor[u'paging'][u'next']
			cursor = json.loads(urllib2.urlopen(next).read())
			like_count += len(cursor[u'data'])
	#Comments
	comment_count = 0
	if u'comments' in post.keys():
		cursor, comment_count = post[u'comments'], len(post[u'comments'][u'data'])
		while u'next' in cursor.keys():
			next = cursor[u'paging'][u'next']
			cursor = json.loads(urllib2.urlopen(next).read())
			comment_count += len(cursor['data'])
	#Shares
	share_count = 0
	if u'shares' in post.keys():
		share_count = post['shares']['count']
	#Current Time Stamp
	ts = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
	#Modified Time Stamp
	modts = float(calendar.timegm(parser.parse(post['updated_time']).utctimetuple())*1000)
	#Stat Dictionary.
	stat = {u'share_count': share_count, u'likes':like_count,u'comments':comment_count,u'last_comment_ts': modts,u'timestamp': ts}	
	return(stat)

def twitter_all_retrieve(keys,df):
	#Returns broken links.
	#If query limit reached, quits and returns the broken links so far. Print the last item.
	import twitter
	import pymongo
	import time
	import calendar
	from dateutil import parser
	from datetime import datetime
	import json
	#Here we go!! 
	print 'Processing Twitter now ... '
	#Filter
	df = df[df['social']=='twitter']
	#Connect
	t = twitter.Twitter(domain='api.twitter.com', api_version='1.1', auth=twitter.oauth.OAuth(keys['twitter']['oauth_token'],keys['twitter']['oauth_secret'],keys['twitter']['consumer_key'],keys['twitter']['consumer_secret']))
	#Database
	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]
	#Collections
	twitter_user = dbtm.twitter_user
	#twitter_status = dbtm.twitter_status
	tweets = dbtm.tweets
	mentions = dbtm.mentions
	retweets = dbtm.retweets
	#Broken Links
	broken = []
	#Count
	tweet_count = 20
	mention_count = 100
	# Collect
	for i,user in df.iterrows():
		try:
			######User LookUp
			result =  t.users.show(screen_name=user['handle'])
			#Insert ID
			result[u'cdpid'] = int(user.name)
			#Insert Date
			result[u'timestamp'] = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
			twitter_user.insert(result)
			print user['handle'], ' inserted'
			#######User Mentions
			time.sleep(2)
			print ' Mentions/ Retweets...'
			mention_results = t.search.tweets(q=user['handle'],count=mention_count)
			for mention in mention_results['statuses']:
				#Check if tweet is a mention or retweet by someone else. Check if the screenname is in entities. 
				#V == 1 --> Mentions
				#V == 2 --> Retweets
				v = twitter_mention_check(mention,user)
				if v == 1:
					query = mentions.find({'_id':str(mention['id'])})
					if query.count() == 0:
						#mention['_id'] = str(mesntion['id'])	
						#Timestamp
						dt = parser.parse(mention['created_at'])
						mention['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
						#Start an Array of CDP IDs. A tweet can mention more than one theater.
						mention[u'cdpid'] = [(int(user.name))]
						#ID
						mention[u'_id'] = str(mention['id'])
						mentions.insert(mention)
						print 'tweeet mention ', mention['_id'],'for user ', user.name, ' inserted'
					#In case if the tweet mention is in the database. Ascertain if the tweet 
					#mentions multiple users.
					#Here I don't put a timestamp to collect retweet or favorite over time. 
					else:
						mention = query.next()
						if not int(user.name) in mention[u'cdpid']:
							print 'tweeet mention ', status['_id'],'for user ', user.name, ' inserted'
							mentions.update({'_id':str(mention['_id'])},{'$push':{'cdpid':int(user.name)}})
						else:
							print 'mention ', mention['id'], ' for user ', user.name, ' in the database.'
				#Retweets
				# elif v == 2:
				# 	query = retweets.find({'_id':str(mention['id'])})
				# 	if query.count() == 0:
				# 		#mention['_id'] = str(mention['id'])	
				# 		#Timestamp
				# 		dt = parser.parse(mention['created_at'])
				# 		mention['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
				# 		#Start an Array of CDP IDs. A tweet can mention more than one theater.
				# 		mention[u'cdpid'] = [(int(user.name))]
				# 		#ID
				# 		mention[u'_id'] = str(mention['id'])
				# 		retweets.insert(mention)
				# 		print 'tweeet retweet ', mention['_id'],'for user ', user.name, ' inserted'
				# 	else:
				# 		print 'retweet ', mention['id'], ' for user ', user.name, ' in the database.'
				else:
					print 'tweet ', mention['id'], ' for user ', user.name, ' not a mention/retweet. Skipping...'
			######Tweets
			#https://dev.twitter.com/docs/api/1.1/get/statuses/user_timeline
			time.sleep(2)
			print 'Tweets ...'
			statuses = t.statuses.user_timeline(screen_name = user['handle'],count=tweet_count)
			for status in statuses:
				#Check if the tweet exits
				query = tweets.find({'_id':str(status['id'])})
				#Timestamp
				ts = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
				#Create an Array of Favorites and Tweets
				stat = {u'favorite_count':status['favorite_count'],u'retweet_count':status['retweet_count'],u'timestamp': ts}
				#Check if it is a retweet. Exclude if it is.
				#Check if the entry in the database already
				if query.count() == 0:
					status['_id'] = str(status['id'])
					status[u'cdpid'] = int(user.name)
					#Timestamp
					dt = parser.parse(status['created_at'])
					status['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
					#Start an Array
					status['stats'] =[stat]
					tweets.insert(status)
					print 'tweeet ', status['_id'],'for user ', user.name, ' inserted'
				else:
					#Push the new stats
					status = query.next()
					print 'tweet ', status['id'], ' for user ', user.name, ' in the database. Updating the statistics..'
					tweets.update({'_id':str(status['_id'])},{'$push':{'stats':stat}})
		except Exception,e:
			print e
			try:
				err = json.loads(e.response_data)
				if 'errors' in err.keys():
					if err['errors'][0]['code']== 88:
						print user.name, ' break. good-bye!'
						return(broken)
						break
				else:
					print user['handle'], ' not retrieved'
					broken.append(user.name)
			except Exception, e:
				print e
				broken.append(user.name)
		#time.sleep(2)
	return(broken)

def twitter_mention_check(mention,user):
	#Returns 0 if retweet or own tweet, 1 else. 
	#Exclude User's Own Tweets and Retweets.
	v = 0
	#Self-Tweet
	if mention['user']['screen_name'].lower() in user['handle'].lower():
		return(v)
	#Entities Check. Search is done by search term but it might be other than the user
	#For example, the_barnes. Barnes Hotel. 
	if 'entities' in mention.keys():
		for user_mentions in mention['entities']['user_mentions']:
			if user_mentions['screen_name'].lower() in user['handle'].lower():
				#Mention
				v = 1
	##Retweet Check. 
	if 'retweeted_status' in mention.keys():
		if mention['retweeted_status']['user']['screen_name'].lower() in user['handle'].lower():
			v = 2
			return(v)
	return(v)


def vimeo_all_retrieve(keys,df):
	import pymongo
	import urllib2
	import json
	import time
	import calendar
	from dateutil import parser
	from datetime import datetime
	import vimeo
	#Here we go!! 
	print 'Processing Vimeo now ... '
	#Connect
	v = vimeo.Client(key=keys['vimeo']['client_id'], secret=keys['vimeo']['client_secret'])
	#Filter
	df = df[df['social']=='vimeo']
	#Database
	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]
	#Collections
	vimeo_video = dbtm.vimeo_video
	vimeo_user = dbtm.vimeo_user
	#Broken Links
	broken = []
	#Collect
	for i,user in df.iterrows():
		try:
			print user.name 
			#Collect All Events
			total,pp = 1,0
			page = 0
			while page * pp <  total:
				#Page Count
				page += 1 
				u = json.loads(v.get('vimeo.activity.happenedToUser',user_id = user['handle'],page=page))
				pp, total = float(u['activities']['perpage']), float(u['activities']['total'])
				#Single activity is passed as dict. Need to convert to list to process
				if int(u['activities']['total']) <=1:
					acts = []
					acts.append(u['activities']['activity'])
				else:
					acts = u['activities']['activity']
				#Retrieve
				for act in acts:
					if vimeo_user.find({'_id':str(act['id'])}).count() == 0: 
						act['_id'] = str(act['id'])
						act[u'cdpid'] = int(user.name)
						#Timestamp
						dt = parser.parse(act['time'])
						act['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
						vimeo_user.insert(act)
						print 'activity ', act['_id'], ' for user ', user.name, ' inserted' 
					else:
						print 'activity ', act['id'], ' for user ', user.name, ' already in the database' 
			#Collect Videos
			#Collect All Event
			total,pp = 1,0
			page = 0
			while page * pp <  total:
				#Page Count
				page +=1
				result = json.loads(v.get('vimeo.videos.getAll',user_id = user['handle'],page=page))
				pp, total = float(result['videos']['perpage']), float(result['videos']['total'])
				videos = result['videos']['video']
				#[u'on_this_page', u'total', u'perpage', u'video', u'page']
				for video in videos:
					#Check if the video exists.
					query = vimeo_video.find({'_id':str(video['id'])})
					#Modified Date Timestamp
					ts = float(calendar.timegm(parser.parse(video['modified_date']).utctimetuple())*1000)
					#Create an Array of Likes, Comments, Plays
					number_of_comments = int(video['number_of_comments'] if 'number_of_comments' in video.keys() else u'0')
					number_of_likes = int(video['number_of_likes'] if 'number_of_likes' in video.keys() else u'0')
					number_of_plays = int(video['number_of_plays'] if 'number_of_plays' in video.keys() else u'0')
					stat = {u'timestamp': ts,u'number_of_comments':number_of_comments,u'number_of_likes': number_of_likes, u'number_of_plays': number_of_plays}
					#If the video is already in the database. Query and push the stat update if there has been one. Else inser. 
					if query.count() == 0:
						video['_id'] = str(video['id'])
						video[u'cdpid'] = int(user.name)
						#Timestamp
						dt = parser.parse(video['upload_date'])
						video['timestamp'] = float(calendar.timegm(dt.utctimetuple())*1000)
						#Start an Array
						video['stats'] = [stat]
						#video['stats'] = [{u'timestamp': ts,u'number_of_comments':video['number_of_comments'],u'number_of_likes': video['number_of_likes'], u'number_of_plays': video['number_of_plays']}]
						vimeo_video.insert(video)
						print 'video', video['_id'], ' for user ', user.name, ' inserted'
					else:
						#Check if the Stats updated given the video is in the database
						video = query.next()
						print 'video ', video['id'], ' for user ', user.name, ' in the database. Updating the statistics..'
						#Create a list of timestmaps
						timestamps = [vd['timestamp'] for vd in video['stats']]
						if not ts in timestamps:
							#Push the stat ino the array
							vimeo_video.update({'_id':video['_id']},{'$push': {'stats': stat}})
							print 'Updated stats for ',video['_id'],  video['modified_date']
						else:
							print 'Video stats up to date'
		except Exception,e:
			print e
			if e.message in u'User not found':
				broken.append(user.name)
		time.sleep(3)
	return(broken)

def foursquare_all_retrieve(keys,df):
	import foursquare
	import pymongo
	import time
	import calendar
	from datetime import datetime
	from dateutil import parser
	import json
	#Here we go!
	print 'Processing Foursquare now ... '
	#Filter
	df = df[df.social=='foursquare']
	#Connect
	f = foursquare.Foursquare(client_id=keys['foursquare']['client_id'], client_secret=keys['foursquare']['client_secret'])
	#Database
	connection = pymongo.MongoClient(keys['db']['host'])
	dbtm = connection[keys['db']['name']]
	fs = dbtm.foursquare
	#Broken Links
	broken =[]
	#Collect
	for i,user in df.iterrows():
		try:
			venue = f.venues(user['handle'])
			venue = venue['venue']
			venue[u'cdpid'] = int(user.name)
			#Insert Date of Insertion
			#float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
			#ts = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
			ts  = float(calendar.timegm(datetime.utcnow().utctimetuple())*1000)
			venue[u'timestamp'] = ts
			fs.insert(venue)	
			print 'User profile',i, user['handle'], ' updated'	
			#Sleep
		except Exception, e:
			print e 
			print user['handle'], i, ' not  retrieved'
			broken.append(i)
		time.sleep(3)	
	return(broken)



if __name__ == "__main__":
	import sys, getopt
	from pandas import read_csv
	import json
	from datetime import datetime
	import time


	#Run command line
	#python ~/Dropbox/code/CDP/social_media_daily.py --keys 'keys' --input 'social'

	try:
		opts,args = getopt.getopt(sys.argv[1:],'k:i:', ['keys=','input='])
	except getopt.GetoptError:
		pass
	#Parse Arguments
	for opt, arg in opts:
		if opt in ('-k','--keys'):
			keyname = arg
		elif opt in ('-i','--input'):
			inputname = arg

	#Read Input File
	print 'Starting processing daily updates'
	print datetime.now()
	try:
		social = read_csv('%s.csv'%(inputname))
		social.index = social.id; social = social.drop(['id'],axis=1)
	except:
		print 'Input file is not found. Exiting now ...'
		sys.exit(0)
	try:
		#Keys
		keys=[]
		with open('%s.json'% (keyname)) as f:
		    for line in f:
		        keys.append(json.loads(line))
		keys = keys[0]
	except:
		print 'Key file is not found. Exiting now....'
		sys.exit(0)

	#Run Daily Updates. Facebook going first because of short access token life!
	#facebook_broken = facebook_all_retrieve(keys,social)
	while True: 
		twitter_broken = twitter_all_retrieve(keys,social)
		vimeo_broken = vimeo_all_retrieve(keys,social)
		foursquare_broken = foursquare_all_retrieve(keys,social)
		print 'Completed processing daily updates ...'
		print ' ......... '
		print datetime.now()
		time.sleep(600)




