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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  // Regex for parsing (<attr1>.<op>.<val1>)~(<attr2>.<op>.<val2>) ... See validateAndExtractSearchList
  private static final String SEARCH_REGEX = "(?:\\\\.|[^~\\\\]++)+";

  // Supported operators for search
  // TODO/TBD: Move to tapis-shared
  private static final Set<String> SEARCH_OPS = new HashSet<>(Arrays.asList("eq","neq","gt","gte","lt","lte","in","nin","like","nlike","between"));

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

  /**
   * Validate a list of search conditions and extract the conditions
   * Search list must have the form  (<cond>)~(<cond>)~ ...
   *    where <cond> = <attr>.<op>.<value>
   * If there is only one condition the surrounding parentheses are optional
   * @param searchListStr - String containing all search conditions
   * @return the list of extracted search conditions
   * @throws IllegalArgumentException if error encountered while parsing.
   */
  public static List<String> validateAndExtractSearchList(String searchListStr) throws IllegalArgumentException
  {
    // TODO: review Smruti's Aloe search code for examples and possible re-use
    var searchList = new ArrayList<String>();
    if (StringUtils.isBlank(searchListStr)) return searchList;
    _log.trace("Parsing SearchList: " + searchListStr);
    // Use a regex pattern to split the string
    // Set delimiter as ~ and escape as \
    // Pattern.quote() does escaping of any special characters that need escaping in the regex
    String escape = Pattern.quote("\\");
    String delimiter = Pattern.quote("~");
    // Parse search string into a list of conditions using a regex and split
//    String regexStr = "(" + // start a match group
//                      "?:" + // match either of
//                        escape + "." + // any escaped character
//                       "|" + // or
//                       "[^" + delimiter + escape + "]++" + // match any char except delim or escape, possessive match
//                        ")" + // end a match group
//                        "+"; // repeat any number of times, ignoring empty results. Use * instead of + to include empty results
    Pattern regexPattern = Pattern.compile(SEARCH_REGEX);
    Matcher regexMatcher = regexPattern.matcher(searchListStr);
    while (regexMatcher.find()) { searchList.add(regexMatcher.group()); }
    // If we found only one match the searchList string may be a single condition that may or may not
    // be surrounded by parentheses. So handle that case.
    if (searchList.size() == 1)
    {
      String cond = searchList.get(0);
      // Add parentheses if not present, check start and end
      // Check for unbalanced parentheses in validateAndExtractSearchCondition
      if (!cond.startsWith("(") && !cond.endsWith(")")) cond = "(" + cond + ")";
      searchList.set(0, cond);
    }

    var retList = new ArrayList<String>();
    // Validate that each condition has the form (<attr>.<op>.<value>)
    for (String cond : searchList)
    {
      // validate condition
      String bareCond = validateAndExtractSearchCondition(cond);
      retList.add(bareCond);
    }
    // Remove any empty matches, e.g. () might have been included one or more times
    retList.removeIf(item -> StringUtils.isBlank(item));
    return retList;
  }

  /**
   *  TODO: This code probably also needed in systemslib for checking on backend. Consider moving to tapis-shared
   *        including list of supported operators as an enum, other stuff?
   * Validate and extract a search condition that must of the form (<attr>.<op>.<value>)
   * @param cond the condition to process
   * @return the validated condition without surrounding parentheses
   * @throws IllegalArgumentException if condition is invalid
   */
  private static String validateAndExtractSearchCondition(String cond) throws IllegalArgumentException
  {
    // TODO: review Smruti's Aloe search code for examples and possible re-use
    if (StringUtils.isBlank(cond) || !cond.startsWith("(") || !cond.endsWith(")"))
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_UNBALANCED", cond);
      throw new IllegalArgumentException(errMsg);
    }
    _log.trace("Validate and extract search condition: " + cond);

    // Validate/extract everything inside ()
    // At this point the condition must have surrounding parentheses. Strip them off.
    String retCond = cond.substring(1, cond.length()-1);

    // A blank string is OK at this point and means we are done
    if (StringUtils.isBlank(retCond)) return retCond;

    // Validate that extracted condition is of the form <attr>.<op>.<value> where
    //       <attr> and <op> may contain only certain characters.
    // TODO/TBD: create a util method for this?
    // Validate and extract <attr> and <op>
    int dot1 = retCond.indexOf('.');
    if (dot1 < 0)
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID", cond);
      throw new IllegalArgumentException(errMsg);
    }
    int dot2 = retCond.indexOf('.', dot1+1);
    if (dot2 < 0)
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID", cond);
      throw new IllegalArgumentException(errMsg);
    }
    String attr = retCond.substring(0, dot1);
    String op = retCond.substring(dot1+1, dot2);
    String val = retCond.substring(dot2+1);
    // <attr>, <op> and <val> must not be empty
    // TODO/TBD: If we support unary operators then maybe <val> can be empty
    if (StringUtils.isBlank(attr) || StringUtils.isBlank(op) || StringUtils.isBlank(val))
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID", cond);
      throw new IllegalArgumentException(errMsg);
    }
    // Verify <attr> and <op> contain valid characters.
    // <attr> must start with [a-zA-Z] and contain only [a-zA-Z0-9_]
    Matcher m = (Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$")).matcher(attr);
    if (!m.find())
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID_ATTR", cond);
      throw new IllegalArgumentException(errMsg);
    }
    // TODO/TBD: we can check valid characters or supported <op>, but no need to do both
    // <op> must start with [a-zA-Z] and contain only [a-zA-Z]
//    m = (Pattern.compile("^[a-zA-Z][a-zA-Z]*$")).matcher(op);
//    if (!m.find())
//    {
//      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID_OP", cond);
//      throw new IllegalArgumentException(errMsg);
//    }
    // Verify <op> is supported.
    if (!SEARCH_OPS.contains(op.toLowerCase()))
    {
      String errMsg = ApiUtils.getMsg("SYSAPI_SEARCHCOND_INVALID_OP", cond);
      throw new IllegalArgumentException(errMsg);
    }
    return retCond;
  }
}
