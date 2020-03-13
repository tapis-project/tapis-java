package edu.utexas.tacc.tapis.systems.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import edu.utexas.tacc.tapis.systems.model.TSystem;
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
    if (threadContext.validate()) return null;

    String msg = MsgUtils.getMsg("TAPIS_INVALID_THREADLOCAL_VALUE", "validate");
    _log.error(msg);
    return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
  }

  /**
   * Check that system exists
   * @param tenantName - name of the tenant
   * @param apiUserId - user name associated with API request
   * @param systemName - name of the system to check
   * @param userName - name of user associated with the perms request, for constructing response msg
   * @param prettyPrint - print flag used to construct response
   * @param opName - operation name, for constructing response msg
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSystemExists(SystemsService systemsService, String tenantName, String apiUserId,
                                           String systemName, String userName, boolean prettyPrint, String opName)
  {
    String msg;
    boolean systemExists;
    try { systemExists = systemsService.checkForSystemByName(tenantName, apiUserId, systemName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsg("SYSAPI_CHECK_ERROR", null, opName, systemName, apiUserId, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!systemExists)
    {
      msg = ApiUtils.getMsg("SYSAPI_NOSYSTEM", opName, systemName, apiUserId);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }

  // TODO service level will be done at the back end, systemslib
  /**
   * Check that requester is either owner or (optionally) apiUserId.
   * @param tenantName - name of the tenant
   * @param apiUserId - user name associated with API request
   * @param systemName - name of the system to check
   * @param opUserName - name of user associated with the operation, if any, for constructing response msg
   * @param opName - operation name, for constructing response msg
   * @param mustBeOwner - indicates if only owner can perform requested operation
   * @param prettyPrint - print flag used to construct response
   * @return - null if all checks OK else Response containing info
   */
//  public static Response checkAuth(SystemsService systemsService, String tenantName, String apiUserId,
//                                   String opName, String systemName, String opUserName, boolean mustBeOwner,
//                                   boolean prettyPrint)
//  {
//    String msg;
//    String owner;
//    if (systemsService == null ||  StringUtils.isBlank(tenantName) || StringUtils.isBlank(apiUserId) || StringUtils.isBlank(systemName))
//    {
//      msg = ApiUtils.getMsg("SYSAPI_NULL_INPUT");
//      _log.error(msg);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//    }
//    // Retrieve owner
//    try { owner = systemsService.getSystemOwner(tenantName, apiUserId, systemName); }
//    catch (Exception e)
//    {
//      msg = ApiUtils.getMsg("SYSAPI_GET_OWNER_ERROR", apiUserId, opName, systemName, opUserName, e.getMessage());
//      _log.error(msg, e);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//    }
//    if (StringUtils.isBlank(owner))
//    {
//      msg = ApiUtils.getMsg("SYSAPI_GET_OWNER_EMPTY", apiUserId, opName, systemName, opUserName);
//      _log.error(msg);
//      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//    }
//
//    // Requester (apiUserId) must be the owner or (optionally) the user associated with the operation
//    if (owner.equals(apiUserId) || (!mustBeOwner && apiUserId.equals(opUserName))) return null;
//
//    // Not authorized
//    msg = ApiUtils.getMsg("SYSAPI_UNAUTH", apiUserId, opName, systemName, opUserName);
//    _log.error(msg);
//    return Response.status(Response.Status.UNAUTHORIZED).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
//  }

  /**
   * Check that both or neither of the secrets are blank.
   * This is for PKI_KEYS and ACCESS_KEY where if one part of the secret is supplied the other must also be supplied
   * @param systemName - name of the system, for constructing response msg
   * @param userName - name of user associated with the perms request, for constructing response msg
   * @param prettyPrint - print flag used to construct response
   * @param secretType - secret type (PKI_KEYS, API_KEY), for constructing response msg
   * @param secretName1 - secret name, for constructing response msg
   * @param secretName2 - secret name, for constructing response msg
   * @param secretVal1 - first secret
   * @param secretVal2 - second secret
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSecrets(String systemName, String userName, boolean prettyPrint, String secretType,
                                      String secretName1, String secretName2, String secretVal1, String secretVal2)
  {
    if ((!StringUtils.isBlank(secretVal1) && StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsg("SYSAPI_CRED_SECRET_MISSING", secretType, secretName2, systemName, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if ((StringUtils.isBlank(secretVal1) && !StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsg("SYSAPI_CRED_SECRET_MISSING", secretType, secretName1, systemName, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }
}
