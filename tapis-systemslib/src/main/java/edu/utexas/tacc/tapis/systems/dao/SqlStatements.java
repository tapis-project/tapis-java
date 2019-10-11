package edu.utexas.tacc.tapis.systems.dao;

final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  static final String CREATE_SYSTEM =
      "INSERT INTO systems (tenant, name, description, owner, host, available, bucket_name, root_dir, job_input_dir, " +
              "job_output_dir, work_dir, scratch_dir, effective_user_id, command_protocol, transfer_protocol) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  // Get all rows.
  static final String SELECT_ALL_SYSTEMS =
      "SELECT id, tenant, name, description, owner, host, available, bucket_name, root_dir, effective_user_id, " +
          "created, updated FROM systems ORDER BY id";
  
  // Get a specific row.
  public static final String SELECT_SYSTEM_BY_NAME =
      "SELECT id, tenant, name, description, owner, host, available, bucket_name, root_dir, effective_user_id, " +
          "created, updated FROM systems WHERE tenant = ? AND name = ?";

  // Delete a system given the name
  public static final String DELETE_SYSTEM_BY_NAME =
      "DELETE FROM systems where tenant = ? AND name = ?";

  // -------------------------
  // --- Command Protocol ----
  // -------------------------
  // Field created is handled by DB.
  static final String CREATE_CMDPROT =
    "INSERT INTO cmd_protocol (mechanism, port, use_proxy, proxy_host, proxy_port) VALUES (CAST(? as command_mech_type), ?, ?, ?, ?) RETURNING id";

  // Get a specific record.
  public static final String SELECT_CMDPROT_BY_VALUE =
      "SELECT id, mechanism, port, use_proxy, proxy_host, proxy_port, created " +
          "FROM cmd_protocol WHERE mechanism = CAST(? as command_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";

  // Delete a record given uniquely identifying values
  public static final String DELETE_CMDPROT_BY_VALUE =
      "DELETE FROM cmd_protocol WHERE mechanism = CAST(? as command_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";

  // -------------------------
  // --- Transfer Protocol ----
  // -------------------------
  // Field created is handled by DB.
  static final String CREATE_TXFPROT =
      "INSERT INTO txf_protocol (mechanism, port, use_proxy, proxy_host, proxy_port) VALUES (CAST(? as transfer_mech_type), ?, ?, ?, ?) RETURNING id";

  // Get a specific record.
  public static final String SELECT_TXFPROT_BY_VALUE =
      "SELECT id, mechanism, port, use_proxy, proxy_host, proxy_port, created " +
          "FROM txf_protocol WHERE mechanism = CAST(? as transfer_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";

  // Delete a record given uniquely identifying values
  public static final String DELETE_TXFPROT_BY_VALUE =
      "DELETE FROM txf_protocol WHERE mechanism = CAST(? as transfer_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";
}
