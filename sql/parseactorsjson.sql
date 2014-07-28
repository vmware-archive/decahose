----------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Changelists
-- ===========
-- Ronert Onst <robst@pivotal.io>
-- July-14, 2014
    ----------------------------------------------------------------------------------------


    preferredUsername text
    displayName text
    links_href text
    links_rel text
    twitterTimeZone text
    image text
    verified boolean
    location_displayName text
    location_objectType text
    statusesCount int
    summary text
    languages text
    utcOffset int
    link text
    followersCount int
    favoritesCount int
    friendsCount int
    listedCount int
    postedTime timestamp with time zone
    id text
    objectType text

    preferredUsername
    displayName
    links_href
    links_rel
    twitterTimeZone
    image
    verified
    location_displayName
    location_objectType
    statusesCount
    summary
    languages
    utcOffset
    link
    followersCount
    favoritesCount
    friendsCount
    listedCount
    postedTime
    id
    objectType


    ----------------------------------------------------------------------------------------
-- 2) Create a table to hold the results of the parsed fields from the JSON blobs
    ----------------------------------------------------------------------------------------

drop table if exists twitter.tweets cascade;
create table twitter.actors
    (
        preferredUsername text
        displayName text
        links_href text
        links_rel text
        twitterTimeZone text
        image text
        verified boolean
        location_displayName text
        location_objectType text
        statusesCount int
        summary text
        languages text
        utcOffset int
        link text
        followersCount int
        favoritesCount int
        friendsCount int
        listedCount int
        postedTime timestamp with time zone
        id text
        objectType text
        )
    distributed by (postedTime);

    ----------------------------------------------------------------------------------------
-- 3) Define User Defined Composite Type to hold result of parsed JSON
    ----------------------------------------------------------------------------------------

drop type if exists twitter.gnip_tweet_columns cascade;
create type twitter.gnip_actors_columns
    as
    (
        preferredUsername text
        displayName text
        links_href text
        links_rel text
        twitterTimeZone text
        image text
        verified boolean
        location_displayName text
        location_objectType text
        statusesCount int
        summary text
        languages text
        utcOffset int
        link text
        followersCount int
        favoritesCount int
        friendsCount int
        listedCount int
        postedTime timestamp with time zone
        id text
        objectType text
        );

    ----------------------------------------------------------------------------------------
-- 4) Define PL/Python function to parse the JSON
    ----------------------------------------------------------------------------------------

drop function if exists twitter.gnip_json_parse(text) cascade;
create or replace function twitter.gnip_json_parse_actors(jsonblob text)
    returns twitter.gnip_actors_columns
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
        'actors_preferredUsername'
        'actors_displayName'
        'actors_links_href'
        'actors_links_rel'
        'actors_twitterTimeZone'
        'actors_image'
        'actors_verified'
        'actors_location_displayName'
        'actors_location_objectType'
        'actors_statusesCount'
        'actors_summary'
        'actors_languages'
        'actors_utcOffset'
        'actors_link'
        'actors_followersCount'
        'actors_favoritesCount'
        'actors_friendsCount'
        'actors_listedCount'
        'actors_postedTime'
        'actors_id'
        'actors_objectType'
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
