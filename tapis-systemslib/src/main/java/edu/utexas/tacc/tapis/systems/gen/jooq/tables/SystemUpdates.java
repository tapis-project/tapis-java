/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables;


import com.google.gson.JsonElement;

import edu.utexas.tacc.tapis.systems.dao.JSONBToJsonElementBinding;
import edu.utexas.tacc.tapis.systems.gen.jooq.Keys;
import edu.utexas.tacc.tapis.systems.gen.jooq.TapisSys;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemUpdatesRecord;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Identity;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row7;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class SystemUpdates extends TableImpl<SystemUpdatesRecord> {

    private static final long serialVersionUID = -1173929654;

    /**
     * The reference instance of <code>tapis_sys.system_updates</code>
     */
    public static final SystemUpdates SYSTEM_UPDATES = new SystemUpdates();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<SystemUpdatesRecord> getRecordType() {
        return SystemUpdatesRecord.class;
    }

    /**
     * The column <code>tapis_sys.system_updates.id</code>. System update request id
     */
    public final TableField<SystemUpdatesRecord, Integer> ID = createField(DSL.name("id"), org.jooq.impl.SQLDataType.INTEGER.nullable(false).defaultValue(org.jooq.impl.DSL.field("nextval('system_updates_id_seq'::regclass)", org.jooq.impl.SQLDataType.INTEGER)), this, "System update request id");

    /**
     * The column <code>tapis_sys.system_updates.system_id</code>. Id of system being updated
     */
    public final TableField<SystemUpdatesRecord, Integer> SYSTEM_ID = createField(DSL.name("system_id"), org.jooq.impl.SQLDataType.INTEGER.nullable(false).defaultValue(org.jooq.impl.DSL.field("nextval('system_updates_system_id_seq'::regclass)", org.jooq.impl.SQLDataType.INTEGER)), this, "Id of system being updated");

    /**
     * The column <code>tapis_sys.system_updates.user_name</code>. User who requested the update
     */
    public final TableField<SystemUpdatesRecord, String> USER_NAME = createField(DSL.name("user_name"), org.jooq.impl.SQLDataType.VARCHAR(60).nullable(false), this, "User who requested the update");

    /**
     * The column <code>tapis_sys.system_updates.operation</code>. Type of update operation
     */
    public final TableField<SystemUpdatesRecord, SystemOperation> OPERATION = createField(DSL.name("operation"), org.jooq.impl.SQLDataType.VARCHAR.nullable(false).asEnumDataType(edu.utexas.tacc.tapis.systems.gen.jooq.enums.OperationType.class), this, "Type of update operation", new org.jooq.impl.EnumConverter<edu.utexas.tacc.tapis.systems.gen.jooq.enums.OperationType, edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation>(edu.utexas.tacc.tapis.systems.gen.jooq.enums.OperationType.class, edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation.class));

    /**
     * The column <code>tapis_sys.system_updates.upd_json</code>. JSON representing the update - with secrets scrubbed
     */
    public final TableField<SystemUpdatesRecord, JsonElement> UPD_JSON = createField(DSL.name("upd_json"), org.jooq.impl.SQLDataType.JSONB.nullable(false), this, "JSON representing the update - with secrets scrubbed", new JSONBToJsonElementBinding());

    /**
     * The column <code>tapis_sys.system_updates.upd_text</code>. Text data supplied by client - secrets should be scrubbed
     */
    public final TableField<SystemUpdatesRecord, String> UPD_TEXT = createField(DSL.name("upd_text"), org.jooq.impl.SQLDataType.VARCHAR, this, "Text data supplied by client - secrets should be scrubbed");

    /**
     * The column <code>tapis_sys.system_updates.created</code>. UTC time for when record was created
     */
    public final TableField<SystemUpdatesRecord, LocalDateTime> CREATED = createField(DSL.name("created"), org.jooq.impl.SQLDataType.LOCALDATETIME.nullable(false).defaultValue(org.jooq.impl.DSL.field("timezone('utc'::text, now())", org.jooq.impl.SQLDataType.LOCALDATETIME)), this, "UTC time for when record was created");

    /**
     * Create a <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates() {
        this(DSL.name("system_updates"), null);
    }

    /**
     * Create an aliased <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates(String alias) {
        this(DSL.name(alias), SYSTEM_UPDATES);
    }

    /**
     * Create an aliased <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates(Name alias) {
        this(alias, SYSTEM_UPDATES);
    }

    private SystemUpdates(Name alias, Table<SystemUpdatesRecord> aliased) {
        this(alias, aliased, null);
    }

    private SystemUpdates(Name alias, Table<SystemUpdatesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    public <O extends Record> SystemUpdates(Table<O> child, ForeignKey<O, SystemUpdatesRecord> key) {
        super(child, key, SYSTEM_UPDATES);
    }

    @Override
    public Schema getSchema() {
        return TapisSys.TAPIS_SYS;
    }

    @Override
    public Identity<SystemUpdatesRecord, Integer> getIdentity() {
        return Keys.IDENTITY_SYSTEM_UPDATES;
    }

    @Override
    public UniqueKey<SystemUpdatesRecord> getPrimaryKey() {
        return Keys.SYSTEM_UPDATES_PKEY;
    }

    @Override
    public List<UniqueKey<SystemUpdatesRecord>> getKeys() {
        return Arrays.<UniqueKey<SystemUpdatesRecord>>asList(Keys.SYSTEM_UPDATES_PKEY);
    }

    @Override
    public List<ForeignKey<SystemUpdatesRecord, ?>> getReferences() {
        return Arrays.<ForeignKey<SystemUpdatesRecord, ?>>asList(Keys.SYSTEM_UPDATES__SYSTEM_UPDATES_SYSTEM_ID_FKEY);
    }

    public Systems systems() {
        return new Systems(this, Keys.SYSTEM_UPDATES__SYSTEM_UPDATES_SYSTEM_ID_FKEY);
    }

    @Override
    public SystemUpdates as(String alias) {
        return new SystemUpdates(DSL.name(alias), this);
    }

    @Override
    public SystemUpdates as(Name alias) {
        return new SystemUpdates(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemUpdates rename(String name) {
        return new SystemUpdates(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemUpdates rename(Name name) {
        return new SystemUpdates(name, null);
    }

    // -------------------------------------------------------------------------
    // Row7 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row7<Integer, Integer, String, SystemOperation, JsonElement, String, LocalDateTime> fieldsRow() {
        return (Row7) super.fieldsRow();
    }
}