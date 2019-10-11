package edu.utexas.tacc.tapis.systems.dao;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.model.TransferProtocol;
import edu.utexas.tacc.tapis.systems.model.TransferProtocol.Mechanism;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/*
 * Class to handle persistence for TransferProtocol objects.
 */
public class TransferProtocolDao extends AbstractDao
{
  /* ********************************************************************** */
  /*                               Fields                                   */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(TransferProtocolDao.class);

//    private final DataSource dataSource;

  /* ********************************************************************** */
  /*                             Constructors                               */
  /* ********************************************************************** */
//  @Inject
//  TransferProtocolDao(DataSource dataSource1) {
//    dataSource = dataSource1;
//  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /** Insert a new record.
   *  Operation is idempotent. If record already exists no update will be made
   *
   * @param mechanism
   * @return Sequence id of object created
   * @throws TapisException on error
   */
  public int create(String mechanism, int port, boolean useProxy, String proxyHost, int proxyPort)
      throws TapisException
  {
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(mechanism))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createTransferProtocol", "mechanism");
      _log.error(msg);
      throw new TapisException(msg);
    }

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    if (proxyHost == null) proxyHost = "";

    // Check for existing record. If present we are done
    TransferProtocol item = get(mechanism, port, useProxy, proxyHost, proxyPort);
    if (item != null) return item.getId();

    // Generated sequence id
    int itemId;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Set the sql command.
      String sql = SqlStatements.CREATE_TXFPROT;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, mechanism);
      pstmt.setInt(2, port);
      pstmt.setBoolean(3, useProxy);
      pstmt.setString(4, proxyHost);
      pstmt.setInt(5, proxyPort);

      // Issue the call.
      pstmt.execute();
      ResultSet rs = pstmt.getResultSet();
      // The generated sequence id should come back in the result
      if (!rs.next())
      {
        String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "txf_protocol");
        _log.error(msg);
        throw new TapisException(msg);
      }
      itemId = rs.getInt(1);

      // Commit the transaction.
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }
      // Log the exception.
      String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "systems_tbl");
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Conditionally return the connection back to the connection pool.
      if (conn != null)
      {
        try
        {
          conn.close();
        }
        catch (Exception e)
        {
          // If commit worked, we can swallow the exception.
          // If not, the commit exception will be thrown.
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
          _log.error(msg, e);
        }
      }
    }
    return itemId;
  }

  /**
   * Retrieve a single record matching specified values.
   * Return null if not found.
   *
   * @param mechanism
   * @param port
   * @param useProxy
   * @param proxyHost
   * @param proxyPort
   * @return item if found, null otherwise
   * @throws TapisException
   */
  public TransferProtocol get(String mechanism, int port, boolean useProxy, String proxyHost, int proxyPort)
      throws TapisException
  {
    // Initialize result.
    TransferProtocol result = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_TXFPROT_BY_VALUE;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, mechanism);
      pstmt.setInt(2, port);
      pstmt.setBoolean(3, useProxy);
      pstmt.setString(4, proxyHost);
      pstmt.setInt(5, proxyPort);

      // Issue the call for the 1 row result set.
      ResultSet rs = pstmt.executeQuery();
      result = populate(rs);

      // Close the result and statement.
      rs.close();
      pstmt.close();

      // Commit the transaction.
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      // TODO Update message to give all values
      String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "TransferProtocol", mechanism, e.getMessage());
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Always return the connection back to the connection pool.
      try
      {
        if (conn != null) conn.close();
      }
      catch (Exception e)
      {
        // If commit worked, we can swallow the exception.
        // If not, the commit exception will be thrown.
        String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
        _log.error(msg, e);
      }
    }

    return result;
  }
  /**
   * Delete a single record matching specified values.
   *
   */
  public int delete(String mechanism, int port, boolean useProxy, String proxyHost, int proxyPort)
      throws TapisException
  {
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(mechanism))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "delete", "mechanism");
      _log.error(msg);
      throw new TapisException(msg);
    }

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Set the sql command.
      String sql = SqlStatements.DELETE_TXFPROT_BY_VALUE;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, mechanism);
      pstmt.setInt(2, port);
      pstmt.setBoolean(3, useProxy);
      pstmt.setString(4, proxyHost);
      pstmt.setInt(5, proxyPort);

      // Issue the call.
      rows = pstmt.executeUpdate();

      // Close out and commit
      pstmt.close();
      conn.commit();
    }
    catch (Exception e)
    {
      // Rollback transaction.
      try
      {
        if (conn != null) conn.rollback();
      }
      catch (Exception e1)
      {
        _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
      }

      // Log the exception.
      String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "TXF_protocol");
      _log.error(msg, e);
      throw new TapisException(msg, e);
    }
    finally
    {
      // Conditionally return the connection back to the connection pool.
      if (conn != null)
        try
        {
          conn.close();
        }
        catch (Exception e)
        {
          // If commit worked, we can swallow the exception.
          // If not, the commit exception will be thrown.
          String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
          _log.error(msg, e);
        }
    }
    return rows;
  }


  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */

  /**
   * Instantiate and populate an object with the results from a single row.
   *
   * @param rs the result set for one job
   * @return the new, fully populated job object or null if the result set is empty
   * @throws TapisJDBCException
   */
  private TransferProtocol populate(ResultSet rs)
      throws TapisJDBCException
  {
    // Quick check.
    if (rs == null) return null;

    // Return null if the results are empty or exhausted.
    // This call advances the cursor.
    try
    {
      if (!rs.next()) return null;
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_RESULT_ACCESS_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }

    // Create the item
    TransferProtocol item = null;
    try
    {
      item = new TransferProtocol(rs.getInt(1),
                                 Mechanism.valueOf(rs.getString(2)),
                                 rs.getInt(3),
                                 rs.getBoolean(4),
                                 rs.getString(5),
                                 rs.getInt(6),
                                 rs.getTimestamp(7).toInstant());
    }
    catch (Exception e)
    {
      String msg = MsgUtils.getMsg("DB_TYPE_CAST_ERROR", e.getMessage());
      _log.error(msg, e);
      throw new TapisJDBCException(msg, e);
    }
    return item;
  }
}
