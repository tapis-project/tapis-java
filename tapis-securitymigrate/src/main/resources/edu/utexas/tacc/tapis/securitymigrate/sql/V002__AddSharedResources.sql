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

-- ----------------------------------------------------------------------------------------
--                                     sk_shared
-- ----------------------------------------------------------------------------------------
-- Shared table
CREATE TABLE sk_shared
(
  id               serial4 PRIMARY KEY,
  tenant           character varying(24) NOT NULL,
  grantor          character varying(60) NOT NULL, 
  grantee          character varying(60) NOT NULL,
  resource_type    character varying(24) NOT NULL, 
  resource_id1     character varying(2048) NOT NULL,
  resource_id2     character varying(2048),
  privilege        character varying(64) NOT NULL,
  created          timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),       
  createdby        character varying(60) NOT NULL,  
  createdby_tenant character varying(24) NOT NULL,
);
ALTER TABLE sk_shared OWNER TO tapis;
ALTER SEQUENCE sk_shared_id_seq RESTART WITH 1;

CREATE UNIQUE INDEX shared_t_r1_r2_g_p_idx ON shared (tenant, resource_type, resource_id1, resource_id2, grantor, grantee, privilege);
CREATE INDEX shared_t_grantor_idx ON shared (tenant, grantor);
CREATE INDEX shared_t_grantee_idx ON shared (tenant, grantee);
CREATE INDEX shared_ct_c_idx ON shared (createdby_tenant, createdby); 

COMMENT ON COLUMN sk_shared.id IS 'Unique shared id';
COMMENT ON COLUMN sk_shared.tenant IS 'Shared tenant';
COMMENT ON COLUMN sk_shared.grantor IS 'User granting the privilege';
COMMENT ON COLUMN sk_shared.grantee IS 'User receiving the privilege';
COMMENT ON COLUMN sk_shared.resource_type IS 'The namespace of the resource';
COMMENT ON COLUMN sk_shared.resource_id1 IS 'Required identifier of the target resource';
COMMENT ON COLUMN sk_shared.resource_id2 IS 'Optional identifier of the target resource';
COMMENT ON COLUMN sk_shared.privilege IS 'Access rights being granted';
COMMENT ON COLUMN sk_shared.created IS 'UTC time record was inserted';
COMMENT ON COLUMN sk_shared.createdby IS 'Service that inserted record';
COMMENT ON COLUMN sk_shared.createdby_tenant IS 'Tenant of the creator';

-- Audit table that is inserted into and selected from, but records are never updated or deleted.
CREATE TABLE sk_shared_audit
(
  id        serial8 PRIMARY KEY,                             
  refid     integer NOT NULL,                             
  refname   character varying(60) NOT NULL,             
  refcol    character varying(60) NOT NULL,              
  change    change_type NOT NULL,                           
  oldvalue  character varying(512),                    
  newvalue  character varying(512),                    
  changed   timestamp without time zone NOT NULL DEFAULT (now() at time zone 'utc'),
  changedby character varying(60) NOT NULL DEFAULT current_user
);
ALTER TABLE sk_shared_audit OWNER TO tapis;
CREATE INDEX sk_shared_audit_refid_idx ON sk_shared_audit (refid);

COMMENT ON COLUMN sk_shared_audit.id IS 'Unique record id';
COMMENT ON COLUMN sk_shared_audit.refid IS 'Id of record in associated table';
COMMENT ON COLUMN sk_shared_audit.refname IS 'Name of record in associated table';
COMMENT ON COLUMN sk_shared_audit.refcol IS 'Name of column that changed';
COMMENT ON COLUMN sk_shared_audit.change IS 'Type of change';
COMMENT ON COLUMN sk_shared_audit.oldvalue IS 'String representation of original value';
COMMENT ON COLUMN sk_shared_audit.newvalue IS 'String representation of new value';
COMMENT ON COLUMN sk_shared_audit.changed IS 'UTC time that associated table was changed';
COMMENT ON COLUMN sk_shared_audit.changedby IS 'User that changed associated table';

-- ****************************************************************************************
--                              PROCEDURES and TRIGGERS
-- ****************************************************************************************  
-- ----------------------------------------------------------------------------------------
--                                     audit_sk_shared
-- ----------------------------------------------------------------------------------------
-- Add owner field auditing.  Replacing the funtion does not break its trigger.
CREATE OR REPLACE FUNCTION audit_sk_shared() RETURNS TRIGGER AS $$
    BEGIN
        --
        -- Some fields may be truncated to fit inside the audit record.
        --
        IF (TG_OP = 'DELETE') THEN
            INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue) 
                VALUES (OLD.id, OLD.grantee, 'ALL', 'delete', 
                        substring(concat(OLD.resource_type, '|', OLD.resource_id1, ':', OLD.resource_id2)) from 1 for 512));
            RETURN OLD;
        ELSIF (TG_OP = 'UPDATE') THEN
            -- We always use the OLD id and, when applicable, OLD name in update audit records, 
            -- even if those fields are among those that have changed.  Any update that changes  
            -- any field will cause an audit record to be written, though there are no update
            -- endpoints planned.   
            IF OLD.id != NEW.id THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'id', 'update', OLD.id::text, NEW.id::text);
            END IF;
            IF OLD.tenant != NEW.tenant THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'tenant', 'update', OLD.tenant, NEW.tenant);
            END IF;
            IF OLD.grantor != NEW.grantor THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'grantor', 'update', OLD.grantor, NEW.grantor);
            END IF;
            IF OLD.grantee != NEW.grantee THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'grantee', 'update', OLD.grantee, NEW.grantee);
            END IF;
            IF OLD.resource_type != NEW.resource_type THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'resource_type', 'update', OLD.resource_type, NEW.resource_type);
            END IF;
            IF OLD.resource_id1 != NEW.resource_id1 THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'resource_id1', 'update', 
                            substring(trim(both ' \b\n\r' from OLD.resource_id1) from 1 for 512), 
                            substring(trim(both ' \b\n\r' from NEW.resource_id1) from 1 for 512));
            END IF;
            IF OLD.resource_id2 != NEW.resource_id2 THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.grantee, 'resource_id2', 'update', 
                            substring(trim(both ' \b\n\r' from OLD.resource_id2) from 1 for 512), 
                            substring(trim(both ' \b\n\r' from NEW.resource_id2) from 1 for 512));
            END IF;
            IF OLD.privilege != NEW.privilege THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'privilege', 'update', OLD.privilege, NEW.privilege);
            END IF;
            IF OLD.createdby != NEW.createdby THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'createdby', 'update', OLD.createdby, NEW.createdby);
            END IF;
            IF OLD.createdby_tenant != NEW.createdby_tenant THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'createdby_tenant', 'update', OLD.createdby_tenant, NEW.createdby_tenant);
            END IF;
            IF OLD.created != NEW.created THEN
                INSERT INTO sk_shared_audit (refid, refname, refcol, change, oldvalue, newvalue) 
                    VALUES (OLD.id, OLD.name, 'created', 'update', OLD.created::text, NEW.created::text);
            END IF;
            RETURN NEW;
        ELSIF (TG_OP = 'INSERT') THEN
            INSERT INTO sk_shared_audit (refid, refname, refcol, change, newvalue) 
                VALUES (NEW.id, NEW.grantee, 'ALL', 'insert', 
                        substring(concat(NEW.resource_type, '|', NEW.resource_id1, ':', NEW.resource_id2)) from 1 for 512));
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_sk_shared_trigger
AFTER INSERT OR UPDATE OR DELETE ON sk_shared
    FOR EACH ROW EXECUTE PROCEDURE audit_sk_shared();
    
    
   