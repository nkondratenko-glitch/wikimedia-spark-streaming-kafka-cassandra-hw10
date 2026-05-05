CREATE KEYSPACE IF NOT EXISTS wikimedia
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wikimedia.page_creations (
    user_id text,
    domain text,
    created_at timestamp,
    page_title text,
    PRIMARY KEY ((domain), created_at, user_id, page_title)
) WITH CLUSTERING ORDER BY (created_at DESC);
