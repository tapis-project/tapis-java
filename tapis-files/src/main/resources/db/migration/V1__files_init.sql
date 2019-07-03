

CREATE TABLE systems (
    id serial primary key,
    tenant_id VARCHAR(256) NOT NULL,
    created_date timestamp  NOT NULL DEFAULT NOW(),
    name VARCHAR(1024) NOT NULL,
    description VARCHAR(1024),
    host VARCHAR(1024) NOT NULL,
    port integer NOT NULL,
    protocol VARCHAR(256) NOT NULL,
    root_dir VARCHAR(1024) NOT NULL
);

CREATE TABLE systems_users (
    id serial primary key,
    system_id integer REFERENCES systems NOT NULL,
    tenant_id VARCHAR(256) NOT NULL,
    username VARCHAR(128) NOT NULL,
    is_owner BOOLEAN DEFAULT FALSE
);



CREATE INDEX idx_files_systems_tenant_id ON systems (tenant_id);
CREATE INDEX idx_files_systems_users_tenant_user ON systems_users (tenant_id, username);


INSERT into systems (tenant_id, name, description, host, protocol, port, root_dir) VALUES (
'test_tenant', 'test.system', 'A test system', 'https://localhost', 'S3', 9000, '/test_bucket'
);

INSERT into systems_users (system_id, tenant_id, username, is_owner) VALUES (
1, 'test_tenant', 'test_user', true
);