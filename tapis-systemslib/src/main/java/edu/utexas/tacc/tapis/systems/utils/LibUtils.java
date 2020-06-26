package edu.utexas.tacc.tapis.systems.utils;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class LibUtils
{
  // Private constructor to make it non-instantiable
  private LibUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(LibUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.systems.lib.SysLibMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsgAuth(String key, AuthenticatedUser authUser, Object... parms)
  {
    // Construct new array of parms. This appears to be most straightforward approach to modify and pass on varargs.
    var newParms = new Object[4 + parms.length];
    newParms[0] = authUser.getTenantId();
    newParms[1] = authUser.getName();
    newParms[2] = authUser.getOboTenantId();
    newParms[3] = authUser.getOboUser();
    System.arraycopy(parms, 0, newParms, 4, parms.length);
    return getMsg(key, newParms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale Locale for message
   * @param key message key
   * @param parms message parameters
   * @return localized message
   */
  public static String getMsg(String key, Locale locale, Object... parms)
  {
    String msgValue = null;

    if (locale == null) locale = Locale.getDefault();

    ResourceBundle bundle = null;
    try { bundle = ResourceBundle.getBundle(MESSAGE_BUNDLE, locale); }
    catch (Exception e)
    {
      _log.error("Unable to find resource message bundle: " + MESSAGE_BUNDLE, e);
    }
    if (bundle != null) try { msgValue = bundle.getString(key); }
    catch (Exception e)
    {
      _log.error("Unable to find key: " + key + " in resource message bundle: " + MESSAGE_BUNDLE, e);
    }

    if (msgValue != null)
    {
      // No problems. If needed fill in any placeholders in the message.
      if (parms != null && parms.length > 0) msgValue = MessageFormat.format(msgValue, parms);
    }
    else
    {
      // There was a problem. Build a message with as much info as we can give.
      StringBuilder sb = new StringBuilder("Key: ").append(key).append(" not found in bundle: ").append(MESSAGE_BUNDLE);
      if (parms != null && parms.length > 0)
      {
        sb.append("Parameters:[");
        for (Object parm : parms) {sb.append(parm.toString()).append(",");}
        sb.append("]");
      }
      msgValue = sb.toString();
    }
    return msgValue;
  }

  /**
   * Return List of transfer methods as a comma delimited list of strings surrounded by curly braces.
   */
  public static String getTransferMethodsAsString(List<TransferMethod> txfrMethods)
  {
    if (txfrMethods == null || txfrMethods.size() == 0) return TSystem.EMPTY_TRANSFER_METHODS_STR;
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < txfrMethods.size()-1; i++)
    {
      sb.append(txfrMethods.get(i).name()).append(",");
    }
    sb.append(txfrMethods.get(txfrMethods.size()-1).name());
    sb.append("}");
    return sb.toString();
  }

  /**
   * Return String[] array of transfer methods
   */
  public static String[] getTransferMethodsAsStringArray(List<TransferMethod> txfrMethods)
  {
    if (txfrMethods == null || txfrMethods.size() == 0) return TSystem.EMPTY_TRANSFER_METHODS_STR_ARRAY;
    return txfrMethods.stream().map(TransferMethod::name).toArray(String[]::new);
  }

  /**
   * Return TransferMethod[] from String[]
   */
  public static List<TransferMethod> getTransferMethodsFromStringArray(String[] txfrMethods)
  {
    if (txfrMethods == null || txfrMethods.length == 0) return Collections.emptyList();
    return Arrays.stream(txfrMethods).map(TransferMethod::valueOf).collect(Collectors.toList());
  }

  /**
   * Log a TAPIS_NULL_PARAMETER exception and throw a TapisException
   */
  public static void logAndThrowNullParmException(String opName, String parmName) throws TapisException
  {
    String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", opName, parmName);
    _log.error(msg);
    throw new TapisException(msg);
  }

  // =============== DB Transaction Management ============================
  /**
   * Close any DB connection related artifacts that are not null
   * @throws SQLException - on sql error
   */
  public static void closeAndCommitDB(Connection conn, PreparedStatement pstmt, ResultSet rs) throws SQLException
  {
    if (rs != null) rs.close();
    if (pstmt != null) pstmt.close();
    if (conn != null) conn.commit();
  }

  /**
   * Roll back a DB transaction and throw an exception
   * This method always throws an exception, either IllegalStateException or TapisException
   */
  public static void rollbackDB(Connection conn, Exception e, String msgKey, Object... parms) throws TapisException
  {
    try
    {
      if (conn != null) conn.rollback();
    }
    catch (Exception e1)
    {
      _log.error(MsgUtils.getMsg("DB_FAILED_ROLLBACK"), e1);
    }

    // If IllegalStateException or TapisException pass it back up
    if (e instanceof IllegalStateException) throw (IllegalStateException) e;
    if (e instanceof TapisException) throw (TapisException) e;

    // Log the exception.
    String msg = MsgUtils.getMsg(msgKey, parms);
    _log.error(msg, e);
    throw new TapisException(msg, e);
  }

  /**
   * Close DB connection, typically called from finally block
   */
  public static void finalCloseDB(Connection conn)
  {
    // Always return the connection back to the connection pool.
    try
    {
      if (conn != null) conn.close();
    }
    catch (Exception e)
    {
      // If commit worked, we can swallow the exception.
      // If not, the commit exception will have been thrown.
      String msg = MsgUtils.getMsg("DB_FAILED_CONNECTION_CLOSE");
      _log.error(msg, e);
    }
  }
}
