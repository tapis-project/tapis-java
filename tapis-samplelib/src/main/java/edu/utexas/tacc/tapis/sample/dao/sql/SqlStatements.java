package edu.utexas.tacc.tapis.sample.dao.sql;

public final class SqlStatements 
{
  // -------------------- Sample ------------------------
  // Basic sample table access.
  
  // Fields that the DB defaults are not inserted on initial creation.
  public static final String CREATE_SAMPLE = 
      "INSERT INTO sample_tbl (text) VALUES (?)";
  
  // Get all rows.
  public static final String SELECT_ALL_SAMPLES =
      "SELECT id, text, updated FROM sample_tbl ORDER BY id";
  
  // Get a specific row.
  public static final String SELECT_SAMPLE_BY_ID =
      "SELECT id, text, updated FROM sample_tbl WHERE id = ?";
  
}