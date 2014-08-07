------------------------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Maintained by: Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 1) Create tables to hold the results of the parsed fields from the JSON blobs
----------------------------------------------------------------------------------------

	drop table if exists twitter.tweets_master cascade;	
	create table twitter.tweets_master
	(
		body text,
		retweetCount int,
		generator text,
		twitter_filter_level text,
		gnip text,
		favoritesCount int,
		object text,
		twitter_lang text,
		twitter_entities text,
		verb text,
		link text,
		provider text,
		postedTime timestamp with time zone,
		id text,
		objectType text,
		actor_preferredUsername text,
		actor_displayName text,
		actor_links text,
		actor_twitterTimeZone text,
		actor_image text,
		actor_verified boolean,
		actor_location_displayName text,
		actor_location_objectType text,
		actor_statusesCount int,
		actor_summary text,
		actor_languages text,
		actor_utcOffset int,
		actor_link text,
		actor_followersCount int,
		actor_favoritesCount int,
		actor_friendsCount int,
		actor_listedCount int,
		actor_postedTime timestamp with time zone,
		actor_id text,
		actor_objectType text			
	) distributed by (postedTime);

----------------------------------------------------------------------------------------
-- 2) Define User Defined Composite Types to hold results of parsed JSON
----------------------------------------------------------------------------------------
	-- Tweet information
	drop type if exists twitter.gnip_tweet_master_columns cascade;
	create type twitter.gnip_tweet_master_columns
	as
	(
		body text,
		retweetCount int,
		generator text,
		twitter_filter_level text,
		gnip text,
		favoritesCount int,
		object text,
		twitter_lang text,
		twitter_entities text,
		verb text,
		link text,
		provider text,
		postedTime timestamp with time zone,
		id text,
		objectType text,
		actor_preferredUsername text,
		actor_displayName text,
		actor_links text,
		actor_twitterTimeZone text,
		actor_image text,
		actor_verified boolean,
		actor_location_displayName text,
		actor_location_objectType text,
		actor_statusesCount int,
		actor_summary text,
		actor_languages text,
		actor_utcOffset int,
		actor_link text,
		actor_followersCount int,
		actor_favoritesCount int,
		actor_friendsCount int,
		actor_listedCount int,
		actor_postedTime timestamp with time zone,
		actor_id text,
		actor_objectType text		
	);

----------------------------------------------------------------------------------------
-- 3) Define PL/Python functions to parse the JSON
----------------------------------------------------------------------------------------

	-- Parse tweets & actor information
    drop function if exists twitter.gnip_json_parse(text) cascade;
    create or replace function twitter.gnip_json_parse(jsonblob text)
    returns twitter.gnip_tweet_master_columns
    as
    $$
    import collections
    import json
    def flatten(d, parent_key=u'', sep=u'_'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping) and k == u'actor':
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
        u'twitter_lang',
        u'twitter_entities',
        u'verb',
        u'link',
        u'provider',
        u'postedTime',
        u'id',
        u'objectType',    
        u'actor_preferredUsername',
        u'actor_displayName',
        u'actor_links',
        u'actor_twitterTimeZone',
        u'actor_image',
        u'actor_verified',
        u'actor_location_displayName',
        u'actor_location_objectType',
        u'actor_statusesCount',
        u'actor_summary',
        u'actor_languages',
        u'actor_utcOffset',
        u'actor_link',
        u'actor_followersCount',
        u'actor_favoritesCount',
        u'actor_friendsCount',
        u'actor_listedCount',
        u'actor_postedTime',
        u'actor_id',
        u'actor_objectType'
    ]
    jsonblob_cleaned = jsonblob.strip().replace(u'\r\n',u'').replace(u'\n',u'') if jsonblob else jsonblob
    jobj = None
    try:
        jobj = flatten(json.loads(jsonblob_cleaned))
    except ValueError:
        jobj = None
    result = []
    for k in keys:
        result.append(jobj[k] if jobj and k in jobj else None)
    return result
    $$language plpythonu;
	
----------------------------------------------------------------------------------------
