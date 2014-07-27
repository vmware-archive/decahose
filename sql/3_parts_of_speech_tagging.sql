------------------------------------------------------------------------------------------------------
--                  Parts of speech tagging of tweets in HAWQ using gp-ark-tweet-nlp                --                   
--                          Srivatsan Ramanujam <sramanujam@gopivotal.com>                          --
------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------
--1) Define a type to hold [tweet_id, token_index, token, tag] items
----------------------------------------------------------------------------------------

drop type if exists token_tag;
create type token_tag
as
(
    indx int, 
    token text,
    tag text
);

----------------------------------------------------------------------------------------
--2) Define PL/Java wrapper function to invoke gp-ark-tweet-nlp's part-of-speech tagger
----------------------------------------------------------------------------------------

drop function if exists twitter.tag_pos(text);
create or replace function twitter.tag_pos(text)
    returns setof token_tag
as 'postagger.nlp.POSTagger.tagTweet'
immutable language pljavau;

----------------------------------------------------------------------------------------
--3) Show parts-of-speech tagging in using gp-ark-tweet-nlp
----------------------------------------------------------------------------------------

select postedtime, 
       (t).indx,
	   (t).token,
	   (t).tag
from
(
	select id,
	       postedtime, 
	       twitter.tag_pos(body) as t
	from
	(
		select id,
		       postedtime, 
		       body 
		from twitter.tweets 
		where twitter_lang='en' 
		limit 10
	)t1
)t2
order by postedtime, (t).indx;

----------------------------------------------------------------------------------------
