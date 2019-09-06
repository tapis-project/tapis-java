package edu.utexas.tacc.tapis.securitymigrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.migrate.TapisJDBCMigrate;
import edu.utexas.tacc.tapis.migrate.TapisJDBCMigrateParms;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;

/** This utility program migrates the TAPIS_DB_NAME using Flyway.  The options for this 
 * program are implemented in MigrateParms, which can be viewed by running this program 
 * with the -help option.
 * 
 * The clean and drop options should not be used in production since they will delete
 * all data in the database.
 * 
 * If the TAPIS_DB_NAME database does not exist, this program will create it and grant 
 * the TAPIS_USER user ALL privileges on it.  If the TAPIS_USER user does not exist,
 * the database will not be created and processing will abort.   
 * 
 * Note that any administrative user--one that can create databases--can be used to
 * connect to the database.
 * 
 * @author rcardone
 */
public final class TapisSecurityJDBCMigrate
 extends TapisJDBCMigrate
{
  /* **************************************************************************** */
  /*                                    Fields                                    */
  /* **************************************************************************** */
  // Local logger.
  private static Logger _log = LoggerFactory.getLogger(TapisSecurityJDBCMigrate.class);

  /* **************************************************************************** */
  /*                                 Constructors                                 */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* constructor:                                                                 */
  /* ---------------------------------------------------------------------------- */
  protected TapisSecurityJDBCMigrate() throws TapisJDBCException
  {
      super(HikariDSGenerator.TAPIS_SEC_DB_NAME);
  }
  
  /* **************************************************************************** */
  /*                                 Public Methods                               */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* main:                                                                        */
  /* ---------------------------------------------------------------------------- */
  /** The standard command line invocation method.
   * 
   * @param args - the arguments defined in MigrateParms and processed by Args4J.
   */
  public static void main(String[] args)
      throws TapisJDBCException
  {
    // Initial log message.
    _log.info(MsgUtils.getMsg("MIGRATE_STARTING"));
    TapisSecurityJDBCMigrate migrate = new TapisSecurityJDBCMigrate();
    migrate.execute(args);
    _log.info(MsgUtils.getMsg("MIGRATE_STOPPING"));
  }

  /* **************************************************************************** */
  /*                             Protected Methods                                */
  /* **************************************************************************** */
  /* ---------------------------------------------------------------------------- */
  /* getParms:                                                                    */
  /* ---------------------------------------------------------------------------- */
  /** Use this package's parameter processing to override that of our superclass.
   * 
   * @param args command line arguments
   * @return the parsed and validated parameters
   * @throws TapisJDBCException
   */
  @Override
  protected TapisJDBCMigrateParms getParms(String[] args) 
    throws TapisJDBCException 
  {return new TapisSecurityJDBCMigrateParms(args);}
}
