----------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Changelists
-- ===========                             
-- Ronert Onst <robst@pivotal.io>
-- July-14, 2014
----------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 2) Create a table to hold the results of the parsed fields from the JSON blobs
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

----------------------------------------------------------------------------------------
-- 3) Define User Defined Composite Type to hold result of parsed JSON
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
-- 4) Define PL/Python function to parse the JSON
----------------------------------------------------------------------------------------

drop function if exists twitter.gnip_json_parse(text) cascade;
create or replace function twitter.gnip_json_parse(jsonblob text)
    returns twitter_ronert.gnip_tweet_columns
    as
    $$
    import collections
    import json

    def flatten(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(flatten(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)
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
        jobj = flatten(json.loads(jsonblob_cleaned))
    except ValueError, e:
        jobj = None
    result = []
    for k in keys:
        result.append(jobj[k] if jobj and jobj.has_key(k) else None)
    return result
    $$ language plpythonu;

----------------------------------------------------------------------------------------
-- 5) Invoke PL/Python function to parse the JSON from the tweets table
----------------------------------------------------------------------------------------

	insert into twitter.tweets
	select (cols).*
	from
	(
		select twitter.gnip_json_parse(tweet_json) as cols
		from twitter.decahose_rawjson_ext
		limit 1000
	)q
	limit 10;

----------------------------------------------------------------------------------------
-- 6) Show some parsed data as columns
----------------------------------------------------------------------------------------

	select postedtime, 
	       id, 
		   body, 
		   retweetcount 
	from twitter.tweets
	limit 10;

----------------------------------------------------------------------------------------
