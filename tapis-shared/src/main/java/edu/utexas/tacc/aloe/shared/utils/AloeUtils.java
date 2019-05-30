package edu.utexas.tacc.aloe.shared.utils;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.aloe.shared.exceptions.AloeException;
import edu.utexas.tacc.aloe.shared.exceptions.recoverable.AloeRecoverableException;
import edu.utexas.tacc.aloe.shared.i18n.MsgUtils;

public class AloeUtils
{
  /* **************************************************************************** */
  /*                                  Constants                                   */
  /* **************************************************************************** */
  // Local logger.
  private static Logger _log = LoggerFactory.getLogger(AloeUtils.class);
  
  // Set the display text for null references.
  public static final String NULL_STRING = "[null]";
  
  // Create the recursive dump styles.  The multiline style is readable by humans,
  // the comparable style is good for comparing the nested values of two objects
  // for equality.
  public static final MultilineRecursiveToStringStyle multiRecursiveStyle = new MultilineRecursiveToStringStyle();
  public static final ComparableRecursiveToStringStyle recursiveStyle = new ComparableRecursiveToStringStyle();
  
  // The aloe version resource path name. Maven 
  // fills in the version number at build time.
  public static final String ALOE_VERSION_FILE = "/aloe.version";
  
  // Used to generate 3 bytes of randomness that fit into 2^24 - 1. 
  private static final int CEILING = 0x1000000;
  
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // The version string read in from the aloe version resource file.
  private static String _aloeVersion;
  
  /* **************************************************************************** */
  /*                                Public Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* toString:                                                                    */
  /* ---------------------------------------------------------------------------- */
  /** Use reflection to construct a string out of the contents of an object.  The 
   * fields of an object and its superclasses are printed to a string along with
   * the class name and its virtual address.  The address information allows one to 
   * distinguish between different instances of a class.
   * 
   * Be careful not to use this method to inadvertently expose passwords.
   * 
   * Here's an example of a serialized Person object:
   * 
   * <pre>
   * Person@182f0db[
   *   name=John Doe
   *   age=33
   *   smoker=false
   * ]
   * </pre>
   * 
   * @param obj the object whose content will be serialized.  Null is allowed.
   * @return the non-static content of the object represented in a string.
   */
  public static String toString(Object obj) 
  {
   // We always return a string.
   if (obj == null) return NULL_STRING;
   String s;
   
   // Sometimes this fails if there are circular references or other conditions
   // that confuse the reflective code. We log problems and still return a string.
   try {s = ReflectionToStringBuilder.toString(obj, multiRecursiveStyle);}
       catch (Exception e) {
           _log.warn(MsgUtils.getMsg("ALOE_INTROSPECTION_ERROR", obj.getClass().getName(), e.getMessage()), e);
           return obj.toString();
       }
   return s; // successful introspection
  }

  /* ---------------------------------------------------------------------------- */
  /* toComparableString:                                                          */
  /* ---------------------------------------------------------------------------- */
  /** Use reflection to construct a string out of the contents of an object.  The 
   * fields of an object and its superclasses are printed to a string along with
   * the class name.  The format is compact and it does not include any information
   * that could distinguish two objects of the same type with the same field values.
   * This latter property allows one to use simple string comparison to determine
   * if two objects contain the same values.
   * 
   * Be careful not to use this method to inadvertently expose passwords.
   *
   * Here's an example of a serialized Person object:
   * 
   * <pre>
   * Person[name=John Doe,age=33,smoker=false]
   * </pre>
   *
   * @param obj the object whose content will be serialized.  Null is allowed.
   * @return the non-static content of the object represented in a string.
   */
  public static String toComparableString(Object obj) 
  {
   if (obj == null) return NULL_STRING;
   return ReflectionToStringBuilder.toString(obj, recursiveStyle); //ToStringStyle.SHORT_PREFIX_STYLE);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* toSqlString:                                                                 */
  /* ---------------------------------------------------------------------------- */
  /** Extract the sql text from the prepared statement.  This method may need to
   * be enhanced to handle different connection pool implementations.
   * 
   * @param pstmt a non-null prepared statement
   * @return the current sql text that the statement represents
   * @throws SQLException on error
   */
  public static String toSqlString(PreparedStatement pstmt) 
    throws SQLException
  {
    return pstmt.unwrap(PreparedStatement.class).toString();
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getUTCTimestamp:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Get the current instant's timestamp in UTC.  
   * 
   * @return a sql UTC timestamp object ready for persisting in the database.
   */
  public static Timestamp getUTCTimestamp()
  {
    // Return the current UTC timestamp for database operations.
    // Maybe there's a simpler way to do this, but just getting the current time
    // in milliseconds causes jvm local time to be saved to the database.  
    return Timestamp.valueOf(LocalDateTime.now(ZoneId.of(ZoneOffset.UTC.getId())));
  }

  /* ---------------------------------------------------------------------------- */
  /* getInstantFromSqlTimestamp:                                                  */
  /* ---------------------------------------------------------------------------- */
  /** Get the instant representation of a UTC timestamp retrieved the database.
   * 
   * @param ts an SQL UTC timestamp 
   * @return the instant also in UTC
   */
  public static Instant getInstantFromSqlTimestamp(Timestamp ts)
  {
    return ts.toLocalDateTime().toInstant(ZoneOffset.UTC);
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getAloeVersion:                                                              */
  /* ---------------------------------------------------------------------------- */
  /** Read the Aloe version from the version file.  The version file contains only
   * the version of this software as written by Maven during the build.  The value
   * is the value hardcoded into the POM file.
   * 
   * @return the current software's version
   */
  public static String getAloeVersion()
  {
    // Assign the version string only on the first time through.
    if (_aloeVersion == null) 
    {
      try (InputStream ins = AloeUtils.class.getResourceAsStream(ALOE_VERSION_FILE)) {
        _aloeVersion = IOUtils.toString(ins, StandardCharsets.UTF_8);
      }
      catch (Exception e) {
        _log.error(MsgUtils.getMsg("ALOE_VERSION_FILE_ERROR", ALOE_VERSION_FILE));
      }
    }
    return _aloeVersion;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getPasswordFromConsole:                                                      */
  /* ---------------------------------------------------------------------------- */
  /** Get the specified user's password masking the input if possible.
   * 
   * @param userid the user account whose credentials are requested.
   * @return the password string.
   */
  public static String getPasswordFromConsole(String userid)
  {
    // Construct prompt.
    String prompt = "Enter the password for userid " + userid + ": ";
    
    // Get the console.
    Console console = System.console();
    
    // Normal command line execution.
    if (console != null) 
    {
      // Use console facilities to hide password.
      console.printf("%s", prompt);
      char[] pwd = console.readPassword();
      if (pwd != null) return new String(pwd);
        else return null;
    }
    
    // When no console is available (like in Eclipse),
    // try using stdin and stdout.
    System.out.print(prompt);
    byte[] bytes = new byte[256];
    try {   
        // Read the input bytes which are not masked.
        int bytesread = System.in.read(bytes);
        return new String(bytes);
      }
      catch (IOException e){}
    
    // We failed to get a password.
    return null;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getInputFromConsole:                                                         */
  /* ---------------------------------------------------------------------------- */
  /** Get the user input if possible.
   * 
   * @param prompt the text to display to get a response from the user
   * @return user input or null if no input was captured
   */
  public static String getInputFromConsole(String prompt)
  {
    // Get the console.
    Console console = System.console();
    
    // Normal command line execution.
    if (console != null) 
    {
      // Use console facilities to hide password.
      console.printf("%s", prompt);
      String input = console.readLine();
      return input;
    }
    
    // When no console is available (like in Eclipse),
    // try using stdin and stdout.
    System.out.print(prompt);
    byte[] bytes = new byte[256];
    try {   
        // Read the input bytes which are not masked.
        int bytesread = System.in.read(bytes);
        return new String(bytes);
      }
      catch (IOException e){}
    
    // We failed to get a password.
    return null;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getLocalHostname:                                                            */
  /* ---------------------------------------------------------------------------- */
  /** Returns the local hostname by resolving the HOSTNAME environment variable.
   * If that variable is not available, it makes a call the linux hostname program.
   * If that fails, "localhost" is returned by default.
   *  
   * @return hostname of current machine
   */
  public static String getLocalHostname()
  {
      // First try the environment variable.
      String hostname = System.getenv("HOSTNAME");
      
      // Call the hostname program to get the configured hostname.
      // This may differ from what's returned by this statement from
      // the original Agave code:
      //
      //    InetAddress.getLocalHost().getHostName();
      //
      // The above code may call out to DNS, which comes with its own
      // set of problems and unpredictability.  For more discussion, see: 
      //  
      // https://stackoverflow.com/questions/7348711/recommended-way-to-get-hostname-in-java
      if (StringUtils.isBlank(hostname)) 
      {
          // Eclipse seems to think the scanner and its inputstream don't get automatically closed.
          // Debugging into the code shows that this is not true, no resources are left open.
          try (Scanner s = new Scanner(Runtime.getRuntime().exec("hostname").getInputStream()).useDelimiter("\\A")) {
              hostname = s.hasNext() ? s.next() : "localhost";
          } catch (Exception e) {
              _log.error(MsgUtils.getMsg("ALOE_LOCAL_HOSTNAME_ERROR", "localhost"));
              hostname = "localhost"; // return something
          }
      }
      
      return hostname;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getIpAddressesFromNetInterface:                                              */
  /* ---------------------------------------------------------------------------- */
  /** Parses local network interface and returns a list of host addresses.
   * 
   * @return list of ip addresses for the host machine
   * @throws Exception on error
   */
  public static List<String> getIpAddressesFromNetInterface() 
   throws Exception 
  {
      List<String> ipAddresses = new ArrayList<String>();
      try 
      {
          Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
          while (networkInterfaces.hasMoreElements()) 
          {
              NetworkInterface ni = networkInterfaces.nextElement();
              if (ni.isUp() && !ni.isLoopback() && !ni.isPointToPoint() && !ni.isVirtual()) 
              {
                  Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
                  while (inetAddresses.hasMoreElements()) 
                  {
                      InetAddress add = inetAddresses.nextElement();
                      String ip = add.getHostAddress();
                      if (IPAddressValidator.validate(ip) 
                              && !StringUtils.startsWithAny(ip, new String[]{"127", ":", "0"})) 
                      {
                          ipAddresses.add(ip);
                      }
                  }
              }
          }
      } catch (Exception e) {
          String msg = MsgUtils.getMsg("ALOE_GET_LOCAL_IP_ADDR");
          _log.error(msg, e);
          throw e;
      }
      
      return ipAddresses;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getMD5LongHash:                                                              */
  /* ---------------------------------------------------------------------------- */
  /** This function generates a MD5 hash from a string, takes the first 8 bytes out 
   * of it and generates a signed integer value out of the byte array assuming the 
   * array to be in BigEndian formatted.  This function is suitable for lightweight
   * hashing duty where even the full md5 is more than what's needed. 
   * 
   * @param str the string to hash.
   * @param the hash value prefix expressed as a long
   */
  public static long getMD5LongHash(String str) throws NoSuchAlgorithmException 
  {     
          MessageDigest md = MessageDigest.getInstance("MD5");
          md.update(str.getBytes());
          return ByteBuffer.wrap(Arrays.copyOf(md.digest(), 8)).asLongBuffer().get();
  }
  
  /* ---------------------------------------------------------------------------- */
  /* transformRawTenantId                                                         */
  /* ---------------------------------------------------------------------------- */
  /** Convert the raw tenant id encode in the http request header key name into
   * the actual tenant id defined in the tenants table.  Nulls and empty strings
   * are passed back as received.
   * 
   * @param rawTenantId tenantId as encoded in the http request header key name
   * @return the transformed tenant id 
   */
  public static String transformRawTenantId(String rawTenantId)
  {
      if (StringUtils.isBlank(rawTenantId)) return rawTenantId;
      String tenantId = rawTenantId.toLowerCase();
      tenantId = tenantId.replace('-', '.');
      tenantId = tenantId.replace('_', '.');
      return tenantId;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* aloeify:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /** Wrap non-aloe exceptions in an AloeException keeping the same error message
   * in the wrapped exception. 
   * 
   * @param e any throwable that we might wrap in an aloe exception
   * @return an AloeException
   */
  public static AloeException aloeify(Exception e){return aloeify(e, null);}
  
  /* ---------------------------------------------------------------------------- */
  /* aloeify:                                                                     */
  /* ---------------------------------------------------------------------------- */
  /** Wrap non-aloe exceptions in an AloeException.  If the msg parameter is non-null
   * then force wrapping even for Aloe exceptions and insert the msg.  If the msg
   * parameter is null then use the original exception's message in the wrapped
   * exception.  
   * 
   * @param e any throwable that we might wrap in an aloe exception
   * @param msg the new message or null
   * @return an AloeException
   */
  public static AloeException aloeify(Throwable e, String msg)
  {
      // Protect ourselves.
      if (e == null) return new AloeException(msg);
      
      // The result exception.
      AloeException aloeException = null;
      
      // -------- Null Message for AloeException
      if ((msg == null) && (e instanceof AloeException)) {
          // Use the exception as-is unless there's a new message.
          aloeException = (AloeException) e;
      }
      // -------- Wrapper for AloeRecoverableException
      else if (e instanceof AloeRecoverableException) {
          // Wrap the recoverable exception in a generic aloe exception.
          // Recoverable exceptions are discovered by search the cause chain
          // using findInChain(), so there's no loss in burying them inside
          // another exception.
          aloeException = new AloeException(msg, e);
      }
      // -------- Wrapper for AloeException
      else if (e instanceof AloeException) 
      {
          // Create a new instance of the same aloe exception type.
          Class<?> cls = e.getClass();
                
          // Get the two argument (msg, cause) constructor that all 
          // AloeException subtypes implement EXCEPT AloeRecoverableExceptions.
          Class<?>[] parameterTypes = {String.class, Throwable.class};
          Constructor<?> cons = null;
          try {cons = cls.getConstructor(parameterTypes);}
               catch (Exception e2) {
                  String msg2 = MsgUtils.getMsg("ALOE_REFLECTION_ERROR", cls.getName(), 
                                                "getConstructor", e.getMessage());
                  _log.error(msg2, e2);
               }
                
          // Use the constructor to assign the result variable.
          if (cons != null) 
              try {aloeException = (AloeException) cons.newInstance(msg, e);}
                  catch (Exception e2) {
                      String msg2 = MsgUtils.getMsg("ALOE_REFLECTION_ERROR", cls.getName(), 
                                                    "newInstance", e.getMessage());
                      _log.error(msg2, e2);
                  }
                
          // If nothing worked create a generic aloe exception wrapper.
          if (aloeException == null) aloeException = new AloeException(msg, e);
      } 
      // -------- Wrapper for Non-AloeException
      else {
          // Wrap all non-AloeExceptions whether or not there's a new message. 
          aloeException = new AloeException(msg == null ? e.getMessage() : msg, e);
      }
      
      return aloeException;
  }

  /* ---------------------------------------------------------------------------- */
  /* findInChain:                                                                 */
  /* ---------------------------------------------------------------------------- */
  /** Given an exception with a chain of zero or more causal exceptions, determine
   * if the exception or any of its causal predecessors are of one of the specified
   * exception class types.  The idea is to determine if an exception or any of its
   * causal predecessors are instances of one of the exception types specified in 
   * the classes parameter. 
   * 
   * If the top level exception matches or if a match is found in the exception's 
   * causal chain to one of the target classes, the matching exception object is
   * returned.  Otherwise, null is returned.
   * 
   * @param ex some exception with a chain of zero or more causal exceptions
   * @param classes the target exception type or types we are searching for
   * @return the first matching type in the exception chain or 
   *         null if no match is found 
   */
  public static Exception findInChain(Exception ex, Class<?>... classes)
  {
      // Is there anything to do?
      if (ex == null || classes == null || classes.length == 0) return null;
      
      // Test whether the exception is assignable to any of the classes.
      for (Class<?> cls : classes) {
          // If the root assignable to the current class, we're done.
          if (cls.isInstance(ex)) return ex;
          
          // See if any of the causal exceptions buried 
          // in the exception chain are assignable.
          Throwable cause = ex.getCause();
          while (cause != null) {
              if (cls.isInstance(cause) && (cause instanceof Exception)) 
                  return (Exception)cause;
              cause = cause.getCause();
          }
      }
      
      // No match in exception chain.
      return null;
  }
  
  /* ---------------------------------------------------------------------------- */
  /* getRandomString:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Generate a pseudo-random base64url string that can be used to identify a 
   * request serviced by a thread.  
   * 
   * @return the 4 character randomized string
   */
  public static String getRandomString() 
  {
      // Get a pseudo-random int value that has its low-order 
      // 24 bits randomized, which is enough to generate a 
      // 4 character base64 string.
      int n = ThreadLocalRandom.current().nextInt(CEILING);
      byte[] b = new byte[3];
      b[2] = (byte) (n);
      n >>>= 8;
      b[1] = (byte) (n);
      n >>>= 8;
      b[0] = (byte) (n);
      
      // Encode the 3 bytes into 4 characters 
      // and avoid any padding.
      return Base64.getUrlEncoder().encodeToString(b);
  }
}
