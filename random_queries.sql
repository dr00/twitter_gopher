--
-- Get all tweets and their entity attributes
--
SELECT * 
FROM 
    tweets as t 
OUTER LEFT JOIN
    media_entities as m 
    ON t.id = m.tweet_id
OUTER LEFT JOIN
    url_entities as u 
    ON t.id = u.tweet_id
OUTER LEFT JOIN
    usermention_entities as um 
    ON t.id = um.tweet_id
OUTER LEFT JOIN
    hashtag_entities as h 
    ON t.id = h.tweet_id
