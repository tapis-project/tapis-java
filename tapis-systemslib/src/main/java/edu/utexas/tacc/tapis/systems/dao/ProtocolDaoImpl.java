package edu.utexas.tacc.tapis.systems.dao;

import com.google.inject.Singleton;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.systems.model.Protocol;
import edu.utexas.tacc.tapis.systems.model.Protocol.AccessMechanism;
import edu.utexas.tacc.tapis.systems.model.Protocol.TransferMechanism;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/*
 * Class to handle persistence for Protocol objects.
 */
@Singleton
public class ProtocolDaoImpl extends AbstractDao implements ProtocolDao
{
  /* ********************************************************************** */
  /*                               Fields                                   */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(ProtocolDaoImpl.class);

  private static final String DEFAULT_TRANSFERMECHANISMS_STR = "{}";

  //    private final DataSource dataSource;

  /* ********************************************************************** */
  /*                             Constructors                               */
  /* ********************************************************************** */
//  @Inject
//  ProtocolDao(DataSource dataSource1) {
//    dataSource = dataSource1;
//  }

  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */

  /** Insert a new record.
   *  Operation is idempotent. If record already exists no update will be made
   *
   * @param accessMechanism
   * @return Sequence id of object created or existing object
   * @throws TapisException on error
   */
  @Override
  public int create(String accessMechanism, String transferMechanisms,
                    int port, boolean useProxy, String proxyHost, int proxyPort)
      throws TapisException
  {
    // ------------------------- Check Input -------------------------
    if (StringUtils.isBlank(accessMechanism))
    {
      String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createProtocol", "accessMechanism");
      _log.error(msg);
      throw new TapisException(msg);
    }
    if (transferMechanisms == null || StringUtils.isBlank(transferMechanisms)) transferMechanisms = DEFAULT_TRANSFERMECHANISMS_STR;

    // Convert nulls to default values. Postgres adheres to sql standard of <col> = null is not the same as <col> is null
    if (proxyHost == null) proxyHost = "";

    // Check for existing record. If present we are done
    // TODO can we still be immutable now that we have a list of TransferMechanism?
//    Protocol item = getByValue(accessMechanism, port, useProxy, proxyHost, proxyPort);
//    if (item != null) return item.getId();

    // Generated sequence id
    int itemId;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Set the sql command.
      String sql = SqlStatements.CREATE_PROTOCOL;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setString(1, accessMechanism);
      pstmt.setString(2, transferMechanisms);
      pstmt.setInt(3, port);
      pstmt.setBoolean(4, useProxy);
      pstmt.setString(5, proxyHost);
      pstmt.setInt(6, proxyPort);

      // Issue the call.
      pstmt.execute();
      ResultSet rs = pstmt.getResultSet();
      // The generated sequence id should come back in the result
      if (!rs.next())
      {
        String msg = MsgUtils.getMsg("DB_INSERT_FAILURE", "protocol");
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
/*
  public Protocol getByValue(String mechanism, int port, boolean useProxy, String proxyHost, int proxyPort)
      throws TapisException
  {
    // Initialize result.
    Protocol result = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_PROTOCOL_BY_VALUE;

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
      String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "Protocol", mechanism, e.getMessage());
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
*/

  /**
   * Retrieve a single record give the id.
   * Return null if not found.
   *
   * @param id
   * @return item if found, null otherwise
   * @throws TapisException
   */
  @Override
  public Protocol getById(int id) throws TapisException
  {
    // Initialize result.
    Protocol result = null;

    // ------------------------- Call SQL ----------------------------
    Connection conn = null;
    try
    {
      // Get a database connection.
      conn = getConnection();

      // Get the select command.
      String sql = SqlStatements.SELECT_PROTOCOL_BY_ID;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, id);

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

      String msg = MsgUtils.getMsg("DB_SELECT_ID_ERROR", "Protocol", "" + id, e.getMessage());
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
   * Delete a single record given the record id
   *
   */
  @Override
  public int delete(int id) throws TapisException
  {
    int rows = -1;
    // ------------------------- Check Input -------------------------
    if (id < 1)
    {
      // # 0 = method name, 1 = parameter name, 2 = value received
      // TAPIS_INVALID_PARAMETER=TAPIS_INVALID_PARAMETER Invalid parameter received by method {0}: {1} = {2}
      String msg = MsgUtils.getMsg("TAPIS_INVALID_PAMETER", "delete", "id", "" + id);
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
      String sql = SqlStatements.DELETE_PROTOCOL_BY_ID;

      // Prepare the statement and fill in the placeholders.
      PreparedStatement pstmt = conn.prepareStatement(sql);
      pstmt.setInt(1, id);

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
      String msg = MsgUtils.getMsg("DB_DELETE_FAILURE", "protocol");
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
  private Protocol populate(ResultSet rs)
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
    Protocol item = null;
    try
    {
      List<TransferMechanism> tmechsList = new ArrayList<>();
      String tmechsStr = rs.getString(3);
      if (tmechsStr != null && !StringUtils.isBlank(tmechsStr))
      {
        // Strip off surrounding braces and convert strings to enums
        // NOTE: All values should be valid due to enforcement of type in DB and json schema validation
        String[] tmechsStrArray = (tmechsStr.substring(1, tmechsStr.length() - 1)).split(",");
        for (String tmech : tmechsStrArray)
        {
          if (!StringUtils.isBlank(tmech)) tmechsList.add(TransferMechanism.valueOf(tmech));
        }
      }
      item = new Protocol(rs.getInt(1),
                          AccessMechanism.valueOf(rs.getString(2)),
                          tmechsList,
                          rs.getInt(4),
                          rs.getBoolean(5),
                          rs.getString(6),
                          rs.getInt(7),
                          rs.getTimestamp(8).toInstant());
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
