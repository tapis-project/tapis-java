package edu.utexas.tacc.tapis.systems.api.utils;

import com.google.gson.JsonElement;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.sharedapi.security.AuthenticatedUser;
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
   * Fill in first 4 parameters with user and tenant info from AuthenticatedUser
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
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
   * Get a localized message using the specified key and parameters. Locale is null.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(key, null, parms);
  }

  /**
   * Get a localized message using the specified locale, key and parameters.
   * If there is a problem an error is logged and a special message is constructed with as much info as can be provided.
   * @param locale - Locale to use when building message. If null use default locale
   * @param key - Key used to lookup message in properties file.
   * @param parms - Parameters for template variables in message
   * @return Resulting message
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
   * @param authenticatedUser - principal user containing tenant and user info
   * @param systemName - name of the system to check
   * @param prettyPrint - print flag used to construct response
   * @param opName - operation name, for constructing response msg
   * @return - null if all checks OK else Response containing info
   */
  public static Response checkSystemExists(SystemsService systemsService, AuthenticatedUser authenticatedUser,
                                           String systemName, boolean prettyPrint, String opName)
  {
    String msg;
    boolean systemExists;
    try { systemExists = systemsService.checkForSystemByName(authenticatedUser, systemName); }
    catch (Exception e)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_CHECK_ERROR", authenticatedUser, systemName, opName, e.getMessage());
      _log.error(msg, e);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if (!systemExists)
    {
      msg = ApiUtils.getMsgAuth("SYSAPI_NOSYSTEM", authenticatedUser, systemName, opName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }

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
  public static Response checkSecrets(AuthenticatedUser authenticatedUser, String systemName, String userName, boolean prettyPrint,
                                      String secretType, String secretName1, String secretName2, String secretVal1, String secretVal2)
  {
    if ((!StringUtils.isBlank(secretVal1) && StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_SECRET_MISSING", authenticatedUser, systemName, secretType, secretName2, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    if ((StringUtils.isBlank(secretVal1) && !StringUtils.isBlank(secretVal2)))
    {
      String msg = ApiUtils.getMsgAuth("SYSAPI_CRED_SECRET_MISSING", authenticatedUser, systemName, secretType, secretName1, userName);
      _log.error(msg);
      return Response.status(Response.Status.BAD_REQUEST).entity(TapisRestUtils.createErrorResponse(msg, prettyPrint)).build();
    }
    return null;
  }

}
