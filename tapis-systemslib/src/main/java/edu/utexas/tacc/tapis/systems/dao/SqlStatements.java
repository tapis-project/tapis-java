package edu.utexas.tacc.tapis.systems.dao;

final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  static final String CREATE_SYSTEM =
      "INSERT INTO systems (tenant, name, description, owner, host, available, bucket_name, root_dir, " +
              "effective_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
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
  // ----- Command Protocol ----
  // -------------------------
  // Fields id, created are handled by DB.
  static final String CREATE_COMMAND_PROTOCOL =
    "INSERT INTO cmd_protocol (mechanism, port, use_proxy, proxy_host, proxy_port, created) VALUES (?, ?, ?, ?, ?, ?)";
}
