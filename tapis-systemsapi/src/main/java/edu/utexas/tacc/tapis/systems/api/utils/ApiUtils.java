package edu.utexas.tacc.tapis.systems.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.service.SystemsService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/*
   Utility class containing general use static methods.
   This class is non-instantiable
 */
public class ApiUtils
{
  // Private constructor to make it non-instantiable
  private ApiUtils() { throw new AssertionError(); }

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(ApiUtils.class);

  // Location of message bundle files
  private static final String MESSAGE_BUNDLE = "edu.utexas.tacc.tapis.systems.api.SysApiMessages";

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */

  /**
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key
   * @param parms
   * @return
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale
   * @param key
   * @param parms
   * @return
   */
  public static String getMsg(String key, Locale locale, Object... parms)
  {
    // TODO: Pull tenant name from thread context and include it in the message
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

  public static String getValS(JsonElement jelem, String defaultVal)
  {
    if (jelem == null) return defaultVal;
    else return jelem.getAsString();
  }

  public static Response checkContext(TapisThreadContext threadContext, boolean prettyPrint)
  {
    // Validate call checks for tenantId, user and accountType
    // If all OK return null, else return error response.
    if (threadContext.validate())
    {
      return null;
    }
    else
    {
      String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
  }

  /**
   * Check that systems exists and apiUser IS or IS NOT the owner.
   * @param tenantName - name of the tenant
   * @param systemName - name of the system to check
   * @param userName - name of user associated with the perms request, for constructing response msg
   * @param prettyPrint - print flag used to construct response
   * @param requesterName - name of the requester, null if check should be skipped
   * @param opName - operation name, for constructing response msg
   * @param mustBeOwner - flag indicating to check if requester IS or IS NOT the owner
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSystemAndOwner(SystemsService systemsService, String tenantName, String systemName,
                                             String userName, boolean prettyPrint, String requesterName, String opName, boolean mustBeOwner)
  {
    Response resp = null;
    String msg;
    // Check if system exists
    boolean systemExists;
    try { systemExists = systemsService.checkForSystemByName(tenantName, systemName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_CHECK_ERROR", null, opName, systemName, userName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!systemExists)
    {
      msg = ApiUtils.getMsg("SYSAPI_NOSYSTEM", opName, systemName, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }

    // If we are skipping owner check then we are done
    if (requesterName == null) return resp;

    // Check if requester IS or IS NOT owner of the system
    // Get the system owner and verify that requester IS or IS NOT the owner
    String owner;
    try { owner = systemsService.getSystemOwner(tenantName, systemName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_GET_OWNER_ERROR", opName, systemName, userName, requesterName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (StringUtils.isBlank(owner))
    {
      msg = ApiUtils.getMsg("SYSAPI_GET_OWNER_EMPTY", opName, systemName, userName, requesterName);
      _log.error(msg);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    // mustBeOwner flag indicates if we are checking that requester IS or IS NOT the owner
    if (mustBeOwner && !owner.equals(requesterName))
    {
      msg = ApiUtils.getMsg("SYSAPI_NOT_OWNER", opName, systemName, userName, requesterName);
      _log.error(msg);
      return Response.status(Response.Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    else if ( !mustBeOwner && owner.equals(requesterName))
    {
      msg = ApiUtils.getMsg("SYSAPI_IS_OWNER", opName, systemName, userName, requesterName);
      _log.error(msg);
      return Response.status(Response.Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return resp;
  }



}
