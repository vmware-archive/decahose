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

drop table if exists twitter.actors cascade;
create table twitter.actors
    (
        preferredUsername text,
        displayName text,
        links_href text,
        links_rel text,
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
-- 3) Define User Defined Composite Type to hold result of parsed JSON
    ----------------------------------------------------------------------------------------

drop type if exists twitter.gnip_actor_columns cascade;
create type twitter.gnip_actor_columns
    as
    (
        preferredUsername text,
        displayName text,
        links_href text,
        links_rel text,
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
-- 4) Define PL/Python function to parse the JSON
    ----------------------------------------------------------------------------------------

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
    'actor_links_href',
    'actor_links_rel',
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
plpy.notice(jsonblob_cleaned)
plpy.notice(jobj)
for k in keys:
    result.append(jobj[k] if jobj and k in jobj else None)
plpy.notice(result)
return result
    $$ language plpythonu;

    ----------------------------------------------------------------------------------------
-- 5) Invoke PL/Python function to parse the JSON from the tweets table
    ----------------------------------------------------------------------------------------

insert into twitter.actors
select (cols).*
from
    (
    select twitter.gnip_json_parse_actors(tweet_json) as cols
    from twitter.decahose_rawjson_ext
        limit 1000
        )q
    limit 1000;

    ----------------------------------------------------------------------------------------
-- 6) Show some parsed data as columns
    ----------------------------------------------------------------------------------------

select *
from twitter.actors
    limit 10;

    ----------------------------------------------------------------------------------------
