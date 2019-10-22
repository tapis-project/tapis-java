package edu.utexas.tacc.tapis.systems.dao;

final class SqlStatements
{
  // -------------------------
  // -------- Systems --------
  // -------------------------
  // Fields id, created, updated are handled by DB.
  static final String CREATE_SYSTEM =
      "INSERT INTO systems (tenant, name, description, owner, host, available, bucket_name, root_dir, job_input_dir, " +
              "job_output_dir, work_dir, scratch_dir, effective_user_id, protocol) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  // Get all rows selecting all attributes.
  static final String SELECT_ALL_SYSTEMS =
    "SELECT s.id, s.tenant, s.name, s.description, s.owner, s.host, s.available, s.bucket_name, s.root_dir, " +
        "s.job_input_dir, s.job_output_dir, s.work_dir, s.scratch_dir, s.effective_user_id, s.protocol, s.created, " +
        "s.updated, p.access_mechanism, p.transfer_mechanisms, p.port, p.use_proxy, p.proxy_host, p.proxy_port, " +
        "p.created as p_created " +
        "FROM systems as s " +
        "INNER JOIN protocol as p ON s.protocol = p.id " +
        "WHERE s.tenant = ?";

  // Get all rows selecting just the name.
  static final String SELECT_ALL_SYSTEM_NAMES =
    "SELECT name FROM systems WHERE tenant = ? ORDER BY name asc";

  // Get a specific row.
  public static final String SELECT_SYSTEM_BY_NAME =
      "SELECT s.id, s.tenant, s.name, s.description, s.owner, s.host, s.available, s.bucket_name, s.root_dir, " +
          "s.job_input_dir, s.job_output_dir, s.work_dir, s.scratch_dir, s.effective_user_id, s.protocol, s.created, " +
          "s.updated, p.access_mechanism, p.transfer_mechanisms, p.port, p.use_proxy, p.proxy_host, p.proxy_port, " +
          "p.created as p_created " +
          "FROM systems as s " +
          "INNER JOIN protocol as p ON s.protocol = p.id " +
          "WHERE s.tenant = ? AND s.name = ?";

  // Delete a system given the name
  public static final String DELETE_SYSTEM_BY_NAME =
      "DELETE FROM systems where tenant = ? AND name = ?";

  // -------------------------
  // --- Protocol ----
  // -------------------------
  // Field created is handled by DB.
  static final String CREATE_PROTOCOL =
    "INSERT INTO protocol (access_mechanism, transfer_mechanisms," +
        "port, use_proxy, proxy_host, proxy_port) VALUES (?::access_mech_type, ?::transfer_mech_type[], ?, ?, ?, ?) RETURNING id";
//        "port, use_proxy, proxy_host, proxy_port) VALUES (CAST(? as access_mech_type), CAST(? as transfer_mech_type[]), ?, ?, ?, ?) RETURNING id";

  // Get a specific record.
//  public static final String SELECT_PROTOCOL_BY_VALUE =
//      "SELECT id, mechanism, port, use_proxy, proxy_host, proxy_port, created " +
//          "FROM protocol WHERE mechanism = CAST(? as access_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";
  public static final String SELECT_PROTOCOL_BY_ID =
      "SELECT id, access_mechanism, transfer_mechanisms, port, use_proxy, proxy_host, proxy_port, created " +
          "FROM protocol WHERE id = ?";

  // Delete a record given uniquely identifying values
//  public static final String DELETE_CMDPROT_BY_VALUE =
//      "DELETE FROM protocol WHERE mechanism = CAST(? as access_mech_type) AND port = ? AND use_proxy = ? AND proxy_host = ? AND proxy_port = ?";

  // Delete a record given the id
  public static final String DELETE_PROTOCOL_BY_ID =
      "DELETE FROM protocol WHERE id = ?";
}
