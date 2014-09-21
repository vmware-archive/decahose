------------------------------------------------------------------------------------------------------
-- Parse tweets from raw JSON table & insert columns into processed table.
-- Maintained by: Srivatsan Ramanujam <sramanujam@gopivotal.com>, Ronert Obst<robst@gopivotal.com>
------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------
-- 1) Create tables to hold the results of the parsed fields from the external PXF tables
----------------------------------------------------------------------------------------

drop table if exists twitter.master cascade;	
create table twitter.master
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
