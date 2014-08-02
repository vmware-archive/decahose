------------------------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Maintained by: Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 1) Create a table to hold the results of the parsed fields from the JSON blobs
----------------------------------------------------------------------------------------

	drop table if exists twitter.tweets cascade;
	create table twitter.tweets
	(
		body text,
		retweetCount int,
		generator text,
		twitter_filter_level text,
		gnip text,
		favoritesCount int,
		object text,
		actor text,
		twitter_lang text,
		twitter_entities text,
		verb text,
		link text,
		provider text,
		postedTime timestamp with time zone,
		id text,
		objectType text
	        ) distributed by (postedTime);

        drop table if exists twitter.actors cascade;
        create table twitter.actors
            (
                preferredUsername text,
                displayName text,
                links text,
                twitterTimeZone text,
                image text,
                verified boolean,
                location_displayName text,
                location_objectType text,
                statusesCount int,
                summary text,
                languages text,
                utcOffset int,
                link text,
                followersCount int,
                favoritesCount int,
                friendsCount int,
                listedCount int,
                postedTime timestamp with time zone,
                id text,
                objectType text
                )
            distributed by (id);

----------------------------------------------------------------------------------------
-- 2) Define User Defined Composite Type to hold result of parsed JSON
----------------------------------------------------------------------------------------

	drop type if exists twitter.gnip_tweet_columns cascade;
	create type twitter.gnip_tweet_columns
	as
	(
		body text,
		retweetCount int,
		generator text,
		twitter_filter_level text,
		gnip text,
		favoritesCount int,
		object text,
		actor text,
		twitter_lang text,
		twitter_entities text,
		verb text,
		link text,
		provider text,
		postedTime timestamp with time zone,
		id text,
		objectType text
	); 

----------------------------------------------------------------------------------------
-- 3) Define PL/Python function to parse the JSON
----------------------------------------------------------------------------------------

	drop function if exists twitter.gnip_json_parse(text) cascade;
	create or replace function twitter.gnip_json_parse(jsonblob text)
	        returns twitter.gnip_tweet_columns
	as
	$$
	    import json
	    keys = [
		    u'body', 
			u'retweetCount', 
			u'generator', 
			u'twitter_filter_level', 
			u'gnip', 
			u'favoritesCount', 
			u'object', 
			u'actor', 
			u'twitter_lang', 
			u'twitter_entities', 
			u'verb', 
			u'link', 
			u'provider', 
			u'postedTime', 
			u'id', 
			u'objectType'
		]
	    jsonblob_cleaned = jsonblob.strip().replace('\r\n','').replace('\n','') if jsonblob else jsonblob
	    jobj = None 
	    try:
	        jobj = json.loads(jsonblob_cleaned)
	    except ValueError, e:
	        jobj = None
	    result = []
	    for k in keys:
	        result.append(jobj[k] if jobj and jobj.has_key(k) else None)
	    return result
	$$ language plpythonu;
	
----------------------------------------------------------------------------------------	
