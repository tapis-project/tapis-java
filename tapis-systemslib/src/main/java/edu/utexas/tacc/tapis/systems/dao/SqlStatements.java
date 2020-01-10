package edu.utexas.tacc.tapis.systems.dao;

final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  // Fields "tags" and "notes" contain JSON type data
  static final String CREATE_SYSTEM =
    "INSERT INTO systems (tenant, name, description, system_type, owner, host, available, effective_user_id, " +
      "access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes, raw_req) " +
      "VALUES (?, ?, ?, ?::system_type_type, ?, ?, ?, ?, ?::access_meth_type, ?, ?, ?::transfer_meth_type[], ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
      "RETURNING id";

  // Get all rows selecting all attributes.
  static final String SELECT_ALL_SYSTEMS =
    "SELECT id, tenant, name, description, system_type, owner, host, available, effective_user_id, " +
      "access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes, created, updated " +
      "FROM systems " +
      "WHERE tenant = ?";

  // Get all rows selecting just the name.
  static final String SELECT_ALL_SYSTEM_NAMES =
    "SELECT name FROM systems WHERE tenant = ? ORDER BY name asc";

  // Get a specific row.
  public static final String SELECT_SYSTEM_BY_NAME =
    "SELECT id, tenant, name, description, system_type, owner, host, available, effective_user_id, " +
      "access_method, bucket_name, root_dir, transfer_methods, port, use_proxy, proxy_host, proxy_port, " +
      "job_can_exec, job_local_working_dir, job_local_archive_dir, job_remote_archive_system, job_remote_archive_dir, " +
      "tags, notes, created, updated " +
      "FROM systems " +
      "WHERE tenant = ? AND name = ?";

  // Delete a system given the name
  public static final String DELETE_SYSTEM_BY_NAME =
    "DELETE FROM systems where tenant = ? AND name = ?";

  // Check for a existence of a record
  public static final String CHECK_FOR_SYSTEM_BY_NAME =
    "SELECT EXISTS(SELECT 1 FROM systems where tenant = ? AND name = ?)";

  // Get system owner
  public static final String SELECT_SYSTEM_OWNER =
    "SELECT owner FROM systems WHERE tenant = ? AND name = ?";
}
