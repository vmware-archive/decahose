------------------------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Maintained by: Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 1) Create tables to hold the results of the parsed fields from the JSON blobs
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
-- 2) Define User Defined Composite Types to hold results of parsed JSON
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

drop type if exists twitter.gnip_actor_columns cascade;
create type twitter.gnip_actor_columns
    as
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
        );


----------------------------------------------------------------------------------------
-- 3) Define PL/Python functions to parse the JSON
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

drop function if exists twitter.gnip_json_parse_actors(text) cascade;
create or replace function twitter.gnip_json_parse_actors(jsonblob text)
    returns twitter.gnip_actor_columns
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
        'actor_preferredUsername',
        'actor_displayName',
        'actor_links',
        'actor_twitterTimeZone',
        'actor_image',
        'actor_verified',
        'actor_location_displayName',
        'actor_location_objectType',
        'actor_statusesCount',
        'actor_summary',
        'actor_languages',
        'actor_utcOffset',
        'actor_link',
        'actor_followersCount',
        'actor_favoritesCount',
        'actor_friendsCount',
        'actor_listedCount',
        'actor_postedTime',
        'actor_id',
        'actor_objectType'
        ]
    jsonblob_cleaned = jsonblob.strip().replace('\r\n','').replace('\n','') if jsonblob else jsonblob
    jobj = None
    try:
    jobj = flatten(json.loads(jsonblob_cleaned))
    except ValueError:
    jobj = None
    result = []
    for k in keys:
    result.append(jobj[k] if jobj and k in jobj else None)
    return result
    $$ language plpythonu;

----------------------------------------------------------------------------------------
