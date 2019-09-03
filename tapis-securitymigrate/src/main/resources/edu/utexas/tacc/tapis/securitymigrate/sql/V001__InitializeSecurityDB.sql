-- This file contains the data definitions required by the Tapis Security Kernel.
-- The content is a valid postgres command file that defines all the
-- tables, indices and other database artifacts needed by the Security Kernel.
--
-- Domain tables are optimized for reading; audit tables for writing.
--
-- TIMEZONE Convention
----------------------
-- All tables in this application conform to the same timezone usage rule:
--
--      All dates, times and timestamps are stored as UTC WITHOUT TIMEZONE information.
--
-- All temporal values written to the database are required to be UTC, all temporal
-- values read from the database can be assumed to be UTC.

-- Types
CREATE TYPE change_type AS ENUM ('insert', 'update', 'delete');

-- ----------------------------------------------------------------------------------------
--                                          USER
-- ----------------------------------------------------------------------------------------
-- User table
CREATE TABLE sk_user
(
  id serial4 PRIMARY KEY, 
  tenant character varying(24) NOT NULL,
  name character varying(60) NOT NULL,         
  description character varying(2048) NOT NULL,
  logon_enabled boolean NOT NULL,
  hashed_password character varying(8192) NOT NULL,   
  active_start timestamp without time zone NOT NULL,  
  active_end timestamp without time zone,             
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),  
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_user OWNER TO tapis;
ALTER SEQUENCE sk_user_id_seq RESTART WITH 100;
CREATE INDEX sk_user_active_idx ON sk_user (active_start, active_end);
CREATE UNIQUE INDEX sk_user_tenant_name_idx ON sk_user (tenant, name);

COMMENT ON COLUMN sk_user.id IS 'Unique user id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_user.name IS 'Unique user name';
COMMENT ON COLUMN sk_user.description IS 'User description';
COMMENT ON COLUMN sk_user.logon_enabled IS 'Whether this account can be used for logon';
COMMENT ON COLUMN sk_user.hashed_password IS 'Securely hashed password plus self-identifying information';
COMMENT ON COLUMN sk_user.active_start IS 'UTC time before which account is inactive';
COMMENT ON COLUMN sk_user.active_end IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_user.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_user.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_user.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_user.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_user_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60) NOT NULL,             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_user_audit OWNER TO tapis;
CREATE INDEX sk_user_audit_refid_idx ON sk_user_audit (refid);

COMMENT ON COLUMN sk_user_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_user_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_user_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_user_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_user_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_user_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_user_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_user_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_user_audit.changedby IS 'User that changed associated table';


-- ----------------------------------------------------------------------------------------
--                                          ROLE
-- ----------------------------------------------------------------------------------------
-- Role table
CREATE TABLE sk_role
(
  id serial4 PRIMARY KEY,
  tenant character varying(24) NOT NULL,
  name character varying(60) NOT NULL,         
  description character varying(2048) NOT NULL,       
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user            
);
ALTER TABLE sk_role OWNER TO tapis;
ALTER SEQUENCE sk_role_id_seq RESTART WITH 100;
CREATE UNIQUE INDEX sk_role_tenant_name_idx ON sk_user (tenant, name);

COMMENT ON COLUMN sk_role.id IS 'Unique role id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_role.name IS 'Unique role name';
COMMENT ON COLUMN sk_role.description IS 'Role description';
COMMENT ON COLUMN sk_role.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_role.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_role.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_role.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_role_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60) NOT NULL,             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_role_audit OWNER TO tapis;
CREATE INDEX sk_role_audit_refid_idx ON sk_role_audit (refid);

COMMENT ON COLUMN sk_role_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_role_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_role_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_role_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_role_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_role_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_role_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_role_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_role_audit.changedby IS 'User that changed associated table';


-- ----------------------------------------------------------------------------------------
--                                       USER_ROLE
-- ----------------------------------------------------------------------------------------
-- User_Role table
CREATE TABLE sk_user_role
(
  id serial4 PRIMARY KEY, 
  tenant character varying(24) NOT NULL,
  user_id integer NOT NULL,   
  role_id integer NOT NULL,                           
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user,
  FOREIGN KEY (user_id) REFERENCES sk_user (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (role_id) REFERENCES sk_role (id) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE sk_user_role OWNER TO tapis;
ALTER SEQUENCE sk_user_role_id_seq RESTART WITH 100;

CREATE UNIQUE INDEX sk_user_role_u_r_idx ON sk_user_role (user_id, role_id);
CREATE UNIQUE INDEX sk_user_role_r_u_idx ON sk_user_role (role_id, user_id);

COMMENT ON COLUMN sk_user_role.id IS 'Unique role/permission mapping id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_user_role.user_id IS 'User id';
COMMENT ON COLUMN sk_user_role.role_id IS 'Role id';
COMMENT ON COLUMN sk_user_role.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_user_role.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_user_role.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_user_role.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_user_role_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60),             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_user_role_audit OWNER TO tapis;
CREATE INDEX sk_user_role_audit_refid_idx ON sk_user_role_audit (refid);

COMMENT ON COLUMN sk_user_role_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_user_role_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_user_role_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_user_role_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_user_role_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_user_role_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_user_role_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_user_role_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_user_role_audit.changedby IS 'User that changed associated table';


-- ----------------------------------------------------------------------------------------
--                                         PERMISSION
-- ----------------------------------------------------------------------------------------
-- Permission table
CREATE TABLE sk_permission
(
  id serial4 PRIMARY KEY,
  tenant character varying(24) NOT NULL,
  name character varying(60) NOT NULL,         
  description character varying(2048) NOT NULL,       
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user            
);
ALTER TABLE sk_permission OWNER TO tapis;
ALTER SEQUENCE sk_permission_id_seq RESTART WITH 100;
CREATE UNIQUE INDEX sk_permission_tenant_name_idx ON sk_user (tenant, name);

COMMENT ON COLUMN sk_permission.id IS 'Unique permission id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_permission.name IS 'Unique permission name';
COMMENT ON COLUMN sk_permission.description IS 'Permission description';
COMMENT ON COLUMN sk_permission.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_permission.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_permission.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_permission.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_permission_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60) NOT NULL,             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_permission_audit OWNER TO tapis;
CREATE INDEX sk_permission_audit_refid_idx ON sk_permission_audit (refid);

COMMENT ON COLUMN sk_permission_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_permission_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_permission_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_permission_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_permission_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_permission_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_permission_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_permission_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_permission_audit.changedby IS 'User that changed associated table';


-- ----------------------------------------------------------------------------------------
--                                      ROLE_PERMISSION
-- ----------------------------------------------------------------------------------------
-- Role_Permission table
CREATE TABLE sk_role_permission
(
  id serial4 PRIMARY KEY, 
  tenant character varying(24) NOT NULL,
  role_id integer NOT NULL,                           
  permission_id integer NOT NULL,                     
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user,            
  FOREIGN KEY (role_id) REFERENCES sk_role (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (permission_id) REFERENCES sk_permission (id) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE sk_role_permission OWNER TO tapis;
ALTER SEQUENCE sk_role_permission_id_seq RESTART WITH 100;

CREATE UNIQUE INDEX sk_role_permission_r_p_idx ON sk_role_permission (role_id, permission_id);
CREATE UNIQUE INDEX sk_role_permission_p_r_idx ON sk_role_permission (permission_id, role_id);

COMMENT ON COLUMN sk_role_permission.id IS 'Unique role/permission mapping id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_role_permission.role_id IS 'Role id';
COMMENT ON COLUMN sk_role_permission.permission_id IS 'Permission id';
COMMENT ON COLUMN sk_role_permission.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_role_permission.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_role_permission.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_role_permission.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_role_permission_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60),             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_role_permission_audit OWNER TO tapis;
CREATE INDEX sk_role_permission_audit_refid_idx ON sk_role_permission_audit (refid);

COMMENT ON COLUMN sk_role_permission_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_role_permission_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_role_permission_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_role_permission_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_role_permission_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_role_permission_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_role_permission_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_role_permission_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_role_permission_audit.changedby IS 'User that changed associated table';


-- ----------------------------------------------------------------------------------------
--                                      ROLE_TREE
-- ----------------------------------------------------------------------------------------
-- Role table
CREATE TABLE sk_role_tree
(
  id serial4 PRIMARY KEY, 
  tenant character varying(24) NOT NULL,
  parent_role_id integer NOT NULL,                    
  child_role_id integer NOT NULL,                     
  created timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby character varying(60) NOT NULL DEFAULT current_user,           
  updated timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  updatedby character varying(60) NOT NULL DEFAULT current_user,           
  CONSTRAINT parent_not_child_cnstr CHECK (parent_role_id != child_role_id),
  FOREIGN KEY (parent_role_id) REFERENCES sk_role (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (child_role_id) REFERENCES sk_role (id) ON DELETE CASCADE ON UPDATE CASCADE
);
ALTER TABLE sk_role_tree OWNER TO tapis;
ALTER SEQUENCE sk_role_tree_id_seq RESTART WITH 100;

CREATE UNIQUE INDEX sk_role_tree_p_c_idx ON sk_role_tree (parent_role_id, child_role_id);
CREATE UNIQUE INDEX sk_role_tree_c_p_idx ON sk_role_tree (child_role_id, parent_role_id);

COMMENT ON COLUMN sk_role_tree.id IS 'Unique role relationship id';
COMMENT ON COLUMN sk_user.tenant IS 'User tenant name';
COMMENT ON COLUMN sk_role_tree.parent_role_id IS 'Role that includes the child role';
COMMENT ON COLUMN sk_role_tree.child_role_id IS 'Role included in the parent role';
COMMENT ON COLUMN sk_role_tree.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_role_tree.createdby IS 'User that inserted record';
COMMENT ON COLUMN sk_role_tree.updated IS 'UTC time record was last updated';
COMMENT ON COLUMN sk_role_tree.updatedby IS 'User that last updated record';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_role_tree_audit
(
  id serial8 PRIMARY KEY,                             
  refid integer NOT NULL,                             
  refname character varying(60),             
  refcol character varying(60) NOT NULL,              
  change change_type NOT NULL,                           
  oldvalue character varying(512),                    
  newvalue character varying(512),                    
  changed timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user           
);
ALTER TABLE sk_role_tree_audit OWNER TO tapis;
CREATE INDEX sk_role_tree_audit_refid_idx ON sk_role_tree_audit (refid);

COMMENT ON COLUMN sk_role_tree_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_role_tree_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_role_tree_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_role_tree_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_role_tree_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_role_tree_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_role_tree_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_role_tree_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_role_tree_audit.changedby IS 'User that changed associated table';

-- ****************************************************************************************
--                              PROCEDURES and TRIGGERS
-- ****************************************************************************************
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_user
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_user() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Description fields are trimmed of whitespace at both ends and may be truncated
        -- to fit inside the audit record.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue) 
                VALUES (OLD.id, OLD.name, 'ALL', 'delete', 
                        substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512));
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.name != NEW.name THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.name, NEW.name);
            END IF;
            IF OLD.description != NEW.description THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'description', 'update', 
                            substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512), 
                            substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            END IF;
            IF OLD.logon_enabled != NEW.logon_enabled THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'logon_enabled', 'update', OLD.logon_enabled, NEW.logon_enabled);
            END IF;
            -- Don't replicate password hash in audit log to limit cypher text exposure.
            IF OLD.hashed_password != NEW.hashed_password THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change) 
                    VALUES (OLD.id, OLD.name, 'hashed_password', 'update');
            END IF;
            IF OLD.active_start != NEW.active_start THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'active_start', 'update', 
                            OLD.active_start::text, 
                            NEW.active_start::text);
            END IF;
            -- Nulls are handled in a funny way...
            IF (OLD.active_end != NEW.active_end) OR 
               (OLD.active_end is NULL AND NEW.active_end is NOT NULL) OR
               (NEW.active_end is NULL AND OLD.active_end is NOT NULL) THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'active_end', 'update', 
                            OLD.active_end::text, 
                            NEW.active_end::text);
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'created', 'update', 
                            OLD.created::text, 
                            NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updated', 'update', 
                            OLD.updated::text, 
                            NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_user_audit (refid, refname, refcol, change, newvalue) 
                VALUES (NEW.id, NEW.name, 'ALL', 'insert', 
                        substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_user_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_user
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_user();

    
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_role
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_role() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Description fields are trimmed of whitespace at both ends and may be truncated
        -- to fit inside the audit record.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue) 
                VALUES (OLD.id, OLD.name, 'ALL', 'delete', 
                        substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512));
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.name != NEW.name THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.name, NEW.name);
            END IF;
            IF OLD.description != NEW.description THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'description', 'update', 
                            substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512), 
                            substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'created', 'update', 
                            OLD.created::text, 
                            NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_role_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updated', 'update', 
                            OLD.updated::text, 
                            NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_role_audit (refid, refname, refcol, change, newvalue) 
                VALUES (NEW.id, NEW.name, 'ALL', 'insert', 
                        substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_role_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_role
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_role();
    
    
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_user_role
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_user_role() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Note that the name field in the audit table is never assigned and defaults to null.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue) 
                VALUES (OLD.id, 'ALL', 'delete', 
                        OLD.user_id::text || ' -> ' || OLD.role_id::text);
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.user_id != NEW.user_id THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'user_id', 'update', OLD.user_id::text, NEW.user_id::text);
            END IF;
            IF OLD.role_id != NEW.role_id THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'role_id', 'update', OLD.role_id::text, NEW.role_id::text);
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'created', 'update', OLD.created::text, NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_user_role_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updated', 'update', OLD.updated::text, NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_user_role_audit (refid, refcol, change, newvalue) 
                VALUES (NEW.id, 'ALL', 'insert', 
                        NEW.user_id::text || ' -> ' || NEW.role_id::text);
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_user_role_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_user_role
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_user_role();
    
    
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_permission
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_permission() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Description fields are trimmed of whitespace at both ends and may be truncated
        -- to fit inside the audit record.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue) 
                VALUES (OLD.id, OLD.name, 'ALL', 'delete', 
                        substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512));
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.name != NEW.name THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.name, NEW.name);
            END IF;
            IF OLD.description != NEW.description THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'description', 'update', 
                            substring(trim(both ' \b\n\r' from OLD.description) from 1 for 512), 
                            substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'created', 'update', 
                            OLD.created::text, 
                            NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_permission_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'updated', 'update', 
                            OLD.updated::text, 
                            NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_permission_audit (refid, refname, refcol, change, newvalue) 
                VALUES (NEW.id, NEW.name, 'ALL', 'insert', 
                        substring(trim(both ' \b\n\r' from NEW.description) from 1 for 512));
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_permission_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_permission
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_permission();
    
    
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_role_permission
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_role_permission() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Note that the name field in the audit table is never assigned and defaults to null.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue) 
                VALUES (OLD.id, 'ALL', 'delete', 
                        OLD.role_id::text || ' -> ' || OLD.permission_id::text);
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.role_id != NEW.role_id THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'role_id', 'update', OLD.role_id::text, NEW.role_id::text);
            END IF;
            IF OLD.permission_id != NEW.permission_id THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'permission_id', 'update', OLD.permission_id::text, NEW.permission_id::text);
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'created', 'update', OLD.created::text, NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_role_permission_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updated', 'update', OLD.updated::text, NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_role_permission_audit (refid, refcol, change, newvalue) 
                VALUES (NEW.id, 'ALL', 'insert', 
                        NEW.role_id::text || ' -> ' || NEW.permission_id::text);
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_role_permission_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_role_permission
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_role_permission();
    
    
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_role_tree
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION audit_sk_role_tree() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Note that the name field in the audit table is never assigned and defaults to null.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue) 
                VALUES (OLD.id, 'ALL', 'delete', 
                        OLD.parent_role_id::text || ' -> ' || OLD.child_role_id::text);
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written.  Since the updated timestamp 
            -- always changes, updates typically cause 2 or more new audit records.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_user_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'name', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.parent_role_id != NEW.parent_role_id THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'parent_role_id', 'update', OLD.parent_role_id::text, NEW.parent_role_id::text);
            END IF;
            IF OLD.child_role_id != NEW.child_role_id THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'child_role_id', 'update', OLD.child_role_id::text, NEW.child_role_id::text);
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.updatedby != NEW.updatedby THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updatedby', 'update', OLD.updatedby, NEW.updatedby);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'created', 'update', OLD.created::text, NEW.created::text);
            END IF;
            IF OLD.updated != NEW.updated THEN
                INSERT INTO sk_role_tree_audit (refid, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, 'updated', 'update', OLD.updated::text, NEW.updated::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_role_tree_audit (refid, refcol, change, newvalue) 
                VALUES (NEW.id, 'ALL', 'insert', 
                        NEW.parent_role_id::text || ' -> ' || NEW.child_role_id::text);
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_role_tree_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_role_tree
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_role_tree();
    
    


   