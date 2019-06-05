

CREATE TABLE jobs (
    id serial primary key,
    tenant_id VARCHAR(256) NOT NULL,
    created_date timestamp  NOT NULL DEFAULT NOW(),
    name VARCHAR(1024) NOT NULL
);


