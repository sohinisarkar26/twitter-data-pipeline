drop table twitter_feed_elonmusk;

create table IF NOT EXISTS
twitter_feed_elonmusk
(
    Text VARCHAR(5000),
    entities VARCHAR(5000),
    normalized_text VARCHAR(500),
    mentions VARCHAR(500),
    retweet_count integer,
    like_count integer,
    impression_count integer,
    created_at timestamp
);

/* Top 5 Elon Musk tweet topics between 2023-11-23 and 2024-02-24 */
WITH number_table AS (
  SELECT row_number() over () :: int as num 
  FROM public.twitter_feed_elonmusk
  where 1=1
  QUALIFY row_number() over () :: int <= (select max(JSON_ARRAY_LENGTH(REPLACE(normalized_text, '''', '"'))) 
                                          from  public.twitter_feed_elonmusk) 
),
stage as (
SELECT 
  e.*,
  JSON_EXTRACT_ARRAY_ELEMENT_TEXT(REPLACE(normalized_text, '''', '"'), n.num - 1) as normalized_text_unnest
FROM 
  public.twitter_feed_elonmusk e
CROSS JOIN 
  number_table n
WHERE 
  n.num <= JSON_ARRAY_LENGTH(REPLACE(normalized_text, '''', '"'))
),
topics as (
select 
normalized_text_unnest as tweet_topic, 
count(*) as count
from stage
group by 1
order by 2 desc
)
select
tweet_topic,
dense_rank() over (order by count desc) as tweet_count_rank
from topics
where 1=1
qualify tweet_count_rank <=5

/*
result - 
tweet_topic     tweet_count_rank
Falcon 9	    1	
Starlink	    2	
Tesla	        3	
Grok	        4	
SpaceX	        5	
*/

/* Top Elon Musk tweet topics by date */
WITH number_table AS (
  SELECT row_number() over () :: int as num 
  FROM public.twitter_feed_elonmusk
  where 1=1
  QUALIFY row_number() over () :: int <= (select max(JSON_ARRAY_LENGTH(REPLACE(normalized_text, '''', '"'))) 
                                          from  public.twitter_feed_elonmusk) 
),
stage as (
SELECT 
  e.*,
  JSON_EXTRACT_ARRAY_ELEMENT_TEXT(REPLACE(normalized_text, '''', '"'), n.num - 1) as normalized_text_unnest
FROM 
  public.twitter_feed_elonmusk e
CROSS JOIN 
  number_table n
WHERE 
  n.num <= JSON_ARRAY_LENGTH(REPLACE(normalized_text, '''', '"'))
)
select 
date(created_at) as created_at,
normalized_text_unnest as tweet_topic, 
count(*) as count
from stage
group by date(created_at),2
order by date(created_at) desc,3 desc;