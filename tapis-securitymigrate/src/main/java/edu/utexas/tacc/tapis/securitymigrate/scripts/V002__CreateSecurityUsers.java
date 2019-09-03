package edu.utexas.tacc.tapis.securitymigrate.scripts;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.shiro.authc.credential.DefaultPasswordService;
import org.apache.shiro.codec.Base64;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.crypto.hash.DefaultHashService;
import org.apache.shiro.util.SimpleByteSource;
import org.apache.shiro.web.env.IniWebEnvironment;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** This file creates the initial administrative shiro user accounts in the database.
 * The migration method calls the shiro password service to hash the password
 * according to defaults set in shiro.ini.
 * 
 * The code in this file is called immediately after the shiro tables and indexes
 * are created so that all administrative users can be established using shiro's 
 * hashing and password management facilities.  Once established, the users can then
 * be assigned roles and permissions directly from simple sql command files.  See
 * VOO3__PopulateShiroAuthDB.sql for details.
 * 
 * @author rich
 *
 */
public class V002__CreateSecurityUsers
  extends BaseJavaMigration
{
  /* *************************************************************************** */
  /*                                  Fields                                     */
  /* *************************************************************************** */
  // Logger uses Logback.xml for configuration.
  private static final Logger log_ = LoggerFactory.getLogger(V002__CreateSecurityUsers.class);
  
  // Shiro configuration path.
  private static final String SHIRO_CONFIG = "classpath:shiro.ini";
  
  // Global tenant name which cannot be inserted using security kernel public apis.
  private static final String GLOBAL_TENANT = "*";
  
  // The array of user account name to create at installation time.  Each entry 
  // is a 4 element array of {tenant, name, description, password}.  User accounts are  
  // assigned id numbers starting at 1 based on order of appearance in this array.
  private static final String[][] users_ = {
        {GLOBAL_TENANT, "sk_admin", "Security kernel administrator", "password"}
  };
  
  /* *************************************************************************** */
  /*                               Public Methods                                */
  /* *************************************************************************** */
  /* --------------------------------------------------------------------------- */
  /* migrate:                                                                    */
  /* --------------------------------------------------------------------------- */
  @Override
  public void migrate(Context ctx) throws Exception
  {
    // --------------------- Shiro Setup -----------------------------
    // Read the shiro.ini file to get the algorithm and private salt values
    IniWebEnvironment iniEnv = new IniWebEnvironment();
    iniEnv.setConfigLocations(SHIRO_CONFIG);
    Ini ini = iniEnv.getIni();
    Section mainSection = ini.getSection("main");
    String hashAlgorithm = mainSection.get("hashService.hashAlgorithmName");
    String privateSalt = mainSection.get("hashService.privateSalt");
    
    // Initialize the default password service.  Note that this code assumes that
    // the default password service class or one that behaves exactly like it with 
    // regard to the hash service has been specified in the shiro.ini file.  If this
    // invariant does not hold, the administrative user ids may become inaccessible.
    DefaultPasswordService passwordService = new DefaultPasswordService();
    
    // Use the algorithm specified in the ini file or the default if none is specified.
    if (hashAlgorithm != null) 
    {
      // Note that if the ini file defines a hash service, it must be assignable to the 
      // default hash service type or a runtime cast exception will be thrown. 
      DefaultHashService hashService = (DefaultHashService) passwordService.getHashService();
      hashService.setHashAlgorithmName(hashAlgorithm);
    }
    
    // Use the private salt specified in the ini file if it exists.
    // The private hash in the ini file must be in base64 representation.
    // It's decoded here into its utf-8 string representation before
    // being passed to the hash service.
    if (privateSalt != null)
    {
      // Note that if the ini file defines a hash service, it must be assignable to the 
      // default hash service type or a runtime cast exception will be thrown. 
      DefaultHashService hashService = (DefaultHashService) passwordService.getHashService();
      String decodedPrivateSalt = Base64.decodeToString(privateSalt.getBytes());
      hashService.setPrivateSalt(new SimpleByteSource(decodedPrivateSalt));
    }
    
    // --------------------- Database Processing ---------------------
    // Get the database connection.
    Connection conn = ctx.getConnection();
    
    // Get the current auto-commit value.
    boolean orginalCommit = conn.getAutoCommit();
    conn.setAutoCommit(false);
    
    // Start the id count at 1.
    int curId = 1;
    
    // Create a table with its indexes for each resource.
    PreparedStatement stmt = null;
    try {
        // Create the insert statement for the all users.
        String sql = "INSERT INTO sk_user" +
                     " (id, tenant, name, description, logon_enabled, hashed_password, active_start)" + 
                     " VALUES (?, ?, ?, ?, ?, ?)";
        stmt = conn.prepareStatement(sql);
        
        // Create the audit table for each FHIR resource.
        for (String[] curUser : users_)
        {
          // Get the password hash.
          String hashedPassword = passwordService.encryptPassword(curUser[3]);
          
          // Assign statement values.  
          stmt.setInt(1, curId++);
          stmt.setString(2, curUser[0]);
          stmt.setString(3, curUser[1]);
          stmt.setString(4, curUser[2]);
          stmt.setBoolean(5, true);
          stmt.setString(6, hashedPassword);
          stmt.setTimestamp(7, getUTCTimestamp());
          
          // Prepare and execute the statement.
          stmt.executeUpdate();
        }
      }
    catch (Exception e)
      {
        String msg = "Unable to create initial database user ids.";
        log_.error(msg, e);
        conn.rollback();
        throw e;
      }
    finally
      {
        // Close the stmt if it exists and swallow any exceptions.
        if (stmt != null) try {stmt.close();} catch (Exception e2){}
      
        // Always reset the auto-commit value.
        conn.setAutoCommit(orginalCommit);
      }
    
    // We get here only on success.
    conn.commit();
    
    // Tracing.
    if (users_.length == 1)
        log_.info("Created 1 user id.");
      else log_.info("Created " + users_.length + " user ids.");
  }
  
  /* *************************************************************************** */
  /*                              Private Methods                                */
  /* *************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getUTCTimestamp:                                                             */
  /* ---------------------------------------------------------------------------- */
  /** Get the current instant's timestamp in UTC.  
   * 
   * @return a sql UTC timestamp object ready for persisting in the database.
   */
  private Timestamp getUTCTimestamp()
  {
    // Return the current UTC timestamp for database operations.
    // Maybe there's a simpler way to do this, but just getting the current time
    // in milliseconds causes jvm local time to saved to the database.  
    return Timestamp.valueOf(LocalDateTime.now(ZoneId.of(ZoneOffset.UTC.getId())));
  }

}
