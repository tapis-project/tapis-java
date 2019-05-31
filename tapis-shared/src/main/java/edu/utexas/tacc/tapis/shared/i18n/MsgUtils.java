package edu.utexas.tacc.tapis.shared.i18n;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgUtils
{
  /* **************************************************************************** */
  /*                                   Constants                                  */
  /* **************************************************************************** */
  // The message file base name.
  private static final String MESSAGE_BUNDLE_NAME = "edu.utexas.tacc.tapis.shared.i18n.TapisMessages";
  
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(MsgUtils.class);
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // The UTF-8 resource bundle wrapper.  This object converts properties values
  // from ISO-8859-1 (Latin1) to UTF-8.
  private static final EncodedResourceBundle encodedBundle = 
      new EncodedResourceBundle(MESSAGE_BUNDLE_NAME);

  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getMsg:                                                                      */
  /* ---------------------------------------------------------------------------- */
  /** Get the message using the current locale associated with a key.  When the
   * message bundle cannot be loaded, the empty string is returned; when key is 
   * not found, the missing key is returned in a message.  Normally, the value of
   * the key is returned with any placeholders filled in.
   * 
   * @param key the message key
   * @param parms optional values for assigning placeholders in value string 
   * @return the localized message
   */
  public static String getMsg(String key, Object... parms)
  {
    return getMsg(Locale.getDefault(), key, parms);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getMsg:                                                                      */
  /* ---------------------------------------------------------------------------- */
  /** Get the message using the specified locale associated with a key.  When the
   * message bundle cannot be loaded, the empty string is returned; when key is 
   * not found, the missing key is returned in a message.  Normally, the value of
   * the key is returned with any placeholders filled in.
   * 
   * @param key the message key
   * @param parms optional values for assigning placeholders in value string 
   * @return the localized message
   */
  public static String getMsg(Locale locale, String key, Object... parms)
  {
    // Get the value of the key.
    String value = "";
    try {value = encodedBundle.getString(locale, key);}
     catch (MissingResourceException e)
      {
        // Still return the key and its parms.
        String s = "Property \"" + key + "\" was not found in bundle " + MESSAGE_BUNDLE_NAME + ".";
        if (parms != null && parms.length > 0) {
            s += " [";
            for (int i = 0; i < parms.length; i++) {
                if (i != 0) s += ", ";
                s += parms[i].toString();
            }
            s += "]";
        }
        return s;
      }
      
    // Fill in any placeholders in the message.
    if (parms.length > 0) value = MessageFormat.format(value, parms);  
    return value;
  }
}
