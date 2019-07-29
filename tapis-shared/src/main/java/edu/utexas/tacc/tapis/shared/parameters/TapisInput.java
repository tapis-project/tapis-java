package edu.utexas.tacc.tapis.shared.parameters;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class collects input parameters for Tapis services at runtime.  Parameters
 * can be passed in using various, hierarchically-organized mechanisms.  The 
 * mechanisms listed here are arranged in increasing priority order so that a 
 * parameter value assigned by a later mechanism overrides any previous assignment
 * of that parameter.
 * 
 *    1. Property file
 *    2. OS environment variables
 *    3. JVM system properties
 * 
 * The TapisEnv.EnvVar enumeration defines all possible parameters that can be 
 * passed to a service via environment variable or system property.  If this
 * approach becomes unwieldy we can change it, but for now it documents in one
 * place all parameters passed in contextually for all services. 
 * 
 * @author rcardone
 */
public final class TapisInput 
{
  /* **************************************************************************** */
  /*                                  Constants                                   */
  /* **************************************************************************** */
  // Local logger.
  private static final Logger _log = LoggerFactory.getLogger(TapisInput.class);
  
  // Properties file name used by all services.
  public static final String SERVICE_PROPERTY_PREFIX = "edu/utexas/tacc/tapis/";
  public static final String SERVICE_PROPERTY_FILE = "service.properties";
  
  /* **************************************************************************** */
  /*                                   Fields                                     */
  /* **************************************************************************** */
  // The map that contains all of a service's key/value inputs.
  private Properties _properties = new Properties();
  
  // Calling service identity that is used when searching for the default resource  
  // file on the classpath.  For example, if "jobs" is passed in, the calling service  
  // should have a resources/jobs/SERVICE_PROPERTY_FILE defined. 
  private String _serviceName;
  
  /* **************************************************************************** */
  /*                                Constructors                                  */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
  public TapisInput(String serviceName)
  {
    // Validate input.
    if (StringUtils.isBlank(serviceName)) {
      String msg = MsgUtils.getMsg("TAPIS__NULL_PARAMETER", "TapisInput", "serviceName");
      _log.error(msg);
      throw new TapisRuntimeException(msg);
    }
    
    // Use this value when searching for the service's default resource file.
    _serviceName = serviceName;
  }
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getInputParameters:                                                          */
  /* ---------------------------------------------------------------------------- */
  /** Assign Tapis input parameters from all sources.  The parameter sources from
   * lowest to highest precedence are:
   * 
   *    1. Property file
   *        - default property file unless user overrides
   *    2. JVM system properties
   *    3. Environment variables
   *    
   * The universe of possible parameters are defined in the TapisEnv.EnvVar enum.
   * 
   * @return a properties object with the effective parameter values
   * @throws TapisException on I/O error
   */
  public Properties getInputParameters() throws TapisException
  {
    // Find the service property file.
    File file = findServicePropertiesFile();
    
    // Read in the key/value pairs from the properties file.
    readServicePropertiesFile(file);
    
    // Read in key/value pairs from the Java system variables.
    readSystemProperties();
    
    // Read in key/value pairs from the OS environment.
    readEnv();
    
    return _properties;
  }

  /* **************************************************************************** */
  /*                                Private Methods                               */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* findServicePropertiesFile:                                                   */
  /* ---------------------------------------------------------------------------- */
  /** Search for the properties file path name from highest to lowest priority.
   * 
   * @return the file object representing the properties file or null for the 
   *          default resource file
   */
  private File findServicePropertiesFile()
  {
    // Check the environment.
    String filename = TapisEnv.get(TapisEnv.EnvVar.TAPIS_SERVICE_PROPERTIES_PATHNAME);
    if (!StringUtils.isBlank(filename)) {
      return new File(filename);
    }
    
    // Check for a system parameter.
    filename = System.getProperty(TapisEnv.EnvVar.TAPIS_SERVICE_PROPERTIES_PATHNAME.getEnvName());
    if (!StringUtils.isBlank(filename)) {
      return new File(filename);
    }
    
    // Return null to indicate that the default resource file should be used.
    return null;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* readServicePropertiesFile:                                                   */
  /* ---------------------------------------------------------------------------- */
  private void readServicePropertiesFile(File file) throws TapisException
  {
    // ---------------- Load Resource File
    // Null file indicates that the service's built-in properties file should be used.
    if (file == null) {
      
      // Service resource files are distinguished by the subdirectory in which they reside.
      String resourcePath = SERVICE_PROPERTY_PREFIX + _serviceName + "/" + SERVICE_PROPERTY_FILE;
      
      // This class is loaded by the same classloader as the calling service and it
      // should be able to find the service.properties resource file defined for the
      // service.  To make sure 
      InputStream ins = TapisInput.class.getClassLoader().getResourceAsStream(resourcePath);
      
      // Did we find our resource file?
      if (ins == null) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PROPERTIES_FILE_NOT_FOUND", resourcePath);
        _log.error(msg);
        throw new TapisException(msg);
      }
      
      // Read the property file and make sure it always gets closed.
      try (InputStream ins2 = ins) {
        _properties.load(ins2);
      }
      catch (Exception e) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PROPERTIES_FILE_LOAD_ERROR", resourcePath);
        _log.error(msg, e);
        throw new TapisException(msg, e);
      }
    }
    // ---------------- Load User-Specified File
    else {
        // Open, read and close the properties file.
        try (BufferedReader rdr = new BufferedReader(new FileReader(file))) {
          _properties.load(rdr);
      }
      catch (Exception e) {
        String msg = MsgUtils.getMsg("TAPIS_SERVICE_PROPERTIES_FILE_LOAD_ERROR", file.getAbsolutePath());
        _log.error(msg, e);
        throw new TapisException(msg, e);
      }
    }
    
    // ---------------- Remove Environment-Only Values
    // Don't inadvertently suck in environment only variables.  Warn the user
    // that the key/value pair will be ignored.  This approach enforces the 
    // env-only contract by avoiding initialization from invalid sources.
    Iterator<Object> it = _properties.keySet().iterator();
    while (it.hasNext()) {
      String key = (String) it.next();
      if (key.startsWith(TapisEnv.ENVONLY_KEY_PREFIX)) {
        String msg = MsgUtils.getMsg("TAPIS_ENVONLY_INVALID_SOURCE", key, "property file");
        _log.warn(msg);
        it.remove();
      }
    }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* readSystemProperties:                                                        */
  /* ---------------------------------------------------------------------------- */
  private void readSystemProperties()
  {
    // Get all the system properties.
    Properties sysProps = System.getProperties();
    
    // Check for values for each possible variable that is not an environment-only variable.
    for (TapisEnv.EnvVar envVar : TapisEnv.EnvVar.values()) {
      if (sysProps.containsKey(envVar.getEnvName()) &&
          !envVar.getEnvName().startsWith(TapisEnv.ENVONLY_KEY_PREFIX))
        _properties.setProperty(envVar.getEnvName(), sysProps.getProperty(envVar.getEnvName()));
    }
  }
  
  /* ---------------------------------------------------------------------------- */
  /* readEnv:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /* OS env variables may be set to match EnvVar.getEnvName() or envVar.name()
   * Precedence is:
   *     EnvVar.getEnvName() - e.g. tapis.envonly.allow.test.query.parms
   *     EnvVar.name()       - e.g. TAPIS_ENVONLY_ALLOW_TEST_QUERY_PARMS
   */
  private void readEnv()
  {
    // Get all defined environment variables.
    Map<String,String> envMap = System.getenv();
    
    // Check for values for each possible environment variable.
    for (TapisEnv.EnvVar envVar : TapisEnv.EnvVar.values()) {
      String propertyName = envVar.getEnvName();
      if (envMap.containsKey(propertyName))
        _properties.setProperty(propertyName, envMap.get(envVar.getEnvName()));
      else if (envMap.containsKey(envVar.name()))
        _properties.setProperty(propertyName, envMap.get(envVar.name()));
    }
  }
  
}
