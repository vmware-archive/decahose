------------------------------------------------------------------------------------------------------
-- Fetch JSON blobs from HDFS through External Tables & PXF.
-- Maintained by: Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 1) Define external HDFS table refercing the JSON stream with PXF Parser
----------------------------------------------------------------------------------------

drop external table if exists twitter.decahose_pxf cascade;
create external table twitter.decahose_pxf
    (
	"body" text,
	"retweetCount" int,
	"generator" text,
	"twitter_filter_level" text,
	"gnip" text,
	"favoritesCount" int,
	"object" text,
	"twitter_lang" text,
	"twitter_entities" text,
	"verb" text,
	"link" text,
	"provider" text,
	"postedTime" text,
	"id" text,
	"objectType" text,
	"actor.preferredUsername" text,
	"actor.displayName" text,
	"actor.links" text,
	"actor.twitterTimeZone" text,
	"actor.image" text,
	"actor.verified" boolean,
	"actor.location.displayName" text,
	"actor.location.objectType" text,
	"actor.statusesCount" int,
	"actor.summary" text,
	"actor.languages" text,
	"actor.utcOffset" int,
	"actor.link" text,
	"actor.followersCount" int,
	"actor.favoritesCount" int,
	"actor.friendsCount" int,
	"actor.listedCount" int,
	"actor.postedTime" text,
	"actor.id" text,
	"actor.objectType" text			
        )
    LOCATION ('pxf://hdm1.gphd.local:50070/user/vatsan/decahose/YEAR/MONTH/DAY/*.txt?PROFILE=JSON')
    FORMAT 'CUSTOM'  (FORMATTER='pxfwritable_import');

----------------------------------------------------------------------------------------
-- 2) Materialize table in HAWQ and parse types
----------------------------------------------------------------------------------------

drop table if exists twitter.master;
create table twitter.master as
select * from twitter.decahose_pxf
    distributed by (id) ;

insert into twitter.master
select 
    "body",
    "retweetCount",
    "generator",
    "twitter_filter_level",
    "gnip",
    "favoritesCount",
    "object",
    "twitter_lang",
    "twitter_entities",
    "verb",
    "link",
    "provider",
    "postedTime"::timestamp with time zone,
    "id",
    "objectType",
    "actor.preferredUsername",
    "actor.displayName",
    "actor.links",
    "actor.twitterTimeZone",
    "actor.image",
    "actor.verified",
    "actor.location.displayName",
    "actor.location.objectType",
    "actor.statusesCount",
    "actor.summary",
    "actor.languages",
    "actor.utcOffset",
    "actor.link",
    "actor.followersCount",
    "actor.favoritesCount",
    "actor.friendsCount",
    "actor.listedCount",
    "actor.postedTime"::timestamp with time zone,
    "actor.id",
    "actor.objectType"			
from twitter.decahose_pxf;
