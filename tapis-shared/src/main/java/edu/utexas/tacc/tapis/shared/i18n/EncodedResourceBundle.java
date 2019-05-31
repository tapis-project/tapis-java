package edu.utexas.tacc.tapis.shared.i18n;

import java.io.UnsupportedEncodingException;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/** The class provides a workaround for the fact that Java's ResourceBundle class treats all
 * property files as ISO-8859-1 encodings.  This is reasonable given that's how property files
 * are specified in Java, but unreasonable given the prevalence of UTF-8 in modern programming.
 *
 * The inconvenience is that codepoints outside of Latin1 have to be escaped using the slash-uXXXX
 * format.  This makes reading properties difficult for humans and useless to translation
 * services.  The workaround implemented here preserves the caching and other benefits of using
 * ResourceBundle while allowing property files to be UTF-8 encoded.  The cost is extra processing
 * and the creation of another string on each retrieval.  
 * 
 * This issue should go away in Java 9.
 * 
 * The code in this class was adapted from code posted in this discussion:
 * 
 *   http://stackoverflow.com/questions/4659929/how-to-use-utf-8-in-resource-properties-with-resourcebundle
 *  
 * @author rcardone
 */
public final class EncodedResourceBundle
{
  // The configuration variables that work for multiple locales.
  // There's no need to use this class unless the file encoding
  // is other than ISO-8859-1.
  private final String baseName;
  private final String fileEncoding;

  /* --------------------------------------------------------------------------- */
  /* constructor:                                                                */
  /* --------------------------------------------------------------------------- */
  public EncodedResourceBundle(String baseName){
      this(baseName, "UTF-8");
  }
  
  /* --------------------------------------------------------------------------- */
  /* constructor:                                                                */
  /* --------------------------------------------------------------------------- */
  public EncodedResourceBundle(String baseName, String fileEncoding){
      this.baseName = baseName;
      this.fileEncoding = fileEncoding;
  }

  /* --------------------------------------------------------------------------- */
  /* getString:                                                                  */
  /* --------------------------------------------------------------------------- */
  /** Look up the value if a key in the locale's bundle and encode that value
   * in the encoding specified for the object.  If the key is not found,
   * a runtime exception is thrown.
   * 
   * The method allows UTF-8 encoded property files to be read.  The property
   * files themselves are human readable in their locale, i.e., there's no
   * need to use utf escape sequences. 
   * 
   * @param locale the locale bundle in which lookups take place
   * @param key the lookup key
   * @return the value associated with the key, transformed according 
   *            to the specified encoding if possible
   * @throws MissingResourceException if the key is not found
   */
  public String getString(Locale locale, String key){
      String value = ResourceBundle.getBundle(baseName, locale).getString(key); 
      try {return new String(value.getBytes("ISO-8859-1"), fileEncoding);}
        catch (UnsupportedEncodingException e) {return value;}
  }
}
