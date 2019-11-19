package edu.utexas.tacc.tapis.systems.dao;

final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  // Fields "tags" and "notes" contain JSON type data
  static final String CREATE_SYSTEM =
    "INSERT INTO systems (tenant, name, description, owner, host, available, bucket_name, root_dir, job_input_dir, " +
      "job_output_dir, work_dir, scratch_dir, effective_user_id, tags, notes, access_mechanism, transfer_mechanisms, " +
      "port, use_proxy, proxy_host, proxy_port) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::access_mech_type, ?::transfer_mech_type[], ?, ?, ?, ?) RETURNING id";

  // Get all rows selecting all attributes.
  static final String SELECT_ALL_SYSTEMS =
    "SELECT id, tenant, name, description, owner, host, available, bucket_name, root_dir, " +
      "job_input_dir, job_output_dir, work_dir, scratch_dir, effective_user_id, tags, notes, " +
      "access_mechanism, transfer_mechanisms, port, use_proxy, proxy_host, " +
      "proxy_port, created, updated " +
      "FROM systems " +
      "WHERE tenant = ?";

  // Get all rows selecting just the name.
  static final String SELECT_ALL_SYSTEM_NAMES =
    "SELECT name FROM systems WHERE tenant = ? ORDER BY name asc";

  // Get a specific row.
  public static final String SELECT_SYSTEM_BY_NAME =
    "SELECT id, tenant, name, description, owner, host, available, bucket_name, root_dir, " +
      "job_input_dir, job_output_dir, work_dir, scratch_dir, effective_user_id, tags, notes, " +
      "access_mechanism, transfer_mechanisms, port, use_proxy, proxy_host, " +
      "proxy_port, created, updated " +
      "FROM systems " +
      "WHERE tenant = ? AND name = ?";

  // Delete a system given the name
  public static final String DELETE_SYSTEM_BY_NAME =
    "DELETE FROM systems where tenant = ? AND name = ?";
}
