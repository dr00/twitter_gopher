CREATE TABLE "bot_followers" 
(
    "observed_date" DATETIME NOT NULL  DEFAULT CURRENT_TIMESTAMP
    , "user_id" INTEGER NOT NULL 
)

CREATE TABLE "bot_friends" 
(
    "observed_date" DATETIME NOT NULL  DEFAULT CURRENT_TIMESTAMP
    , "user_id" INTEGER NOT NULL 
)

CREATE TABLE "target_users" ("user_id" INTEGER NOT NULL )
