package edu.utexas.tacc.tapis.systems.dao;

/*
 * SQL strings for all systems operations
 */
final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  static final String CREATE_SYSTEM =
    "INSERT INTO systems (tenant, name, description, system_type, owner, host, enabled, effective_user_id, " +
      "default_access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes_jsonb) " +
      "VALUES (?, ?, ?, ?::system_type_type, ?, ?, ?, ?, ?::access_meth_type, ?, ?, ?::transfer_meth_type[], ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
      "RETURNING id";

  static final String UPDATE_SYSTEM =
    "UPDATE systems SET description = ?, host = ?, enabled = ?, effective_user_id = ?, " +
      "default_access_method = ?::access_meth_type, transfer_methods = ?::transfer_meth_type[], " +
      "port = ?, use_proxy = ?, proxy_host = ?, proxy_port = ?, tags = ?, notes_jsonb = ? " +
      "WHERE id = ?";

  static final String UPDATE_SYSTEM_OWNER =
      "UPDATE systems SET owner = ? WHERE id = ?";

  static final String ADD_UPDATE =
    "INSERT INTO system_updates (system_id, user_name, operation, upd_json, upd_text) VALUES (?, ?, ?::operation_type, ?, ?)";

  // Get all rows selecting all attributes.
  static final String SELECT_ALL_SYSTEMS =
    "SELECT id, tenant, name, description, system_type, owner, host, enabled, effective_user_id, " +
      "default_access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes_jsonb, deleted, created, updated " +
      "FROM systems " +
      "WHERE tenant = ? AND deleted = false ORDER BY name ASC";

  // Get all rows selecting just the name.
  static final String SELECT_ALL_SYSTEM_NAMES =
    "SELECT name FROM systems WHERE tenant = ? AND deleted = false ORDER BY name ASC";

  // Get a specific row.
  static final String SELECT_SYSTEM_BY_NAME =
    "SELECT id, tenant, name, description, system_type, owner, host, enabled, effective_user_id, " +
      "default_access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes_jsonb, deleted, created, updated " +
      "FROM systems " +
      "WHERE tenant = ? AND name = ? AND deleted = false";

  // Soft delete a system given the id
  static final String SOFT_DELETE_SYSTEM_BY_ID =
    "UPDATE systems SET deleted = true WHERE id = ?";

  // Hard delete a system given the name
  static final String HARD_DELETE_SYSTEM_BY_NAME =
    "DELETE FROM systems WHERE tenant = ? AND name = ?";

  // Check for a existence of a record
  static final String CHECK_FOR_SYSTEM_BY_NAME =
    "SELECT EXISTS(SELECT 1 FROM systems WHERE tenant = ? AND name = ? AND deleted = false)";
  static final String CHECK_FOR_SYSTEM_BY_NAME_ALL =
    "SELECT EXISTS(SELECT 1 FROM systems WHERE tenant = ? AND name = ?)";

  // Get system owner
  static final String SELECT_SYSTEM_OWNER =
    "SELECT owner FROM systems WHERE tenant = ? AND name = ?";

  // Get system effectiveuserid
  static final String SELECT_SYSTEM_EFFECTIVEUSERID =
    "SELECT effective_user_id FROM systems WHERE tenant = ? AND name = ?";

  // Get system id
  static final String SELECT_SYSTEM_ID =
    "SELECT id FROM systems WHERE tenant = ? AND name = ?";

  // -------------------------
  // ------ Capabilities -----
  // -------------------------
  static final String ADD_CAPABILITY =
    "INSERT INTO capabilities (system_id, category, name, value) " +
      "VALUES (?, ?::capability_category_type, ?, ?) ";

  static final String SELECT_SYSTEM_CAPS =
    "SELECT category, name, value, created, updated " +
      "FROM capabilities " +
      "WHERE system_id = ?";

  // Delete capabilities for a system
  static final String DELETE_CAPABILITES =
    "DELETE FROM capabilities WHERE system_id = ?";
}
