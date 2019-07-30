package edu.utexas.tacc.tapis.shareddb;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Parse and validate the CreateDatabase parameters, which define a new
 * Tapis database.  The userid parameter must represent a user that has
 * database create permission.  The same userid is made the owner of the
 * new database.
 * 
 * The host parameter defaults to localhost.  
 *   
 * @author rcardone
 */
public class CreateDatabaseParameters 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(CreateDatabaseParameters.class);
    
    // Database defaults.
    private static final String DB_USER = "tapis";
    private static final String DB_PWD  = "password";
    private static final String DB_HOST = "localhost";
    private static final int    DB_PORT = 5432;
    
    // The default database names.
    private static final String DFT_ADMIN_DB = "postgres";
    private static final String DFT_TAPIS_DB = "tapisdb";
    
    // The user doesn't control this.
    static final String DFT_CONNECTION_POOL_SIZE = "2";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // --------- Parameters that define the databases
    @Option(name = "-admindb", required = false,  
        metaVar = "<existing db>", usage = "existing admin db")
    public String adminDB = DFT_ADMIN_DB;
    
    @Option(name = "-tapisdb", required = false, 
            metaVar = "<new db>", usage = "tapis db to be created")
    public String tapisDB = DFT_TAPIS_DB;
    
    // --------- Parameters for database access
    @Option(name = "-dbuser", required = false, usage = "DB user name")
    public String dbUser = DB_USER;

    @Option(name = "-dbpwd", required = false, usage = "DB password")
    public String dbPwd = DB_PWD;

    @Option(name = "-dbhost", required = false, usage = "DB host")
    public String dbHost = DB_HOST;

    @Option(name = "-dbport", required = false, usage = "DB port")
    public int dbPort = DB_PORT;

    @Option(name = "-help", aliases = {"--help"}, 
            usage = "display help information")
    public boolean help;
        
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public CreateDatabaseParameters(String[] args) 
     throws TapisException
    {
      initializeParms(args);
      validateParms();
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* initializeParms:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Parse the input arguments. */
    private void initializeParms(String[] args)
        throws TapisException
    {
      // Get a command line parser to verify input.
      CmdLineParser parser = new CmdLineParser(this);
      parser.getProperties().withUsageWidth(120);
      
      try {
         // Parse the arguments.
         parser.parseArgument(args);
        }
       catch (CmdLineException e)
        {
         if (!help)
           {
            // Create message buffer of sufficient size.
            final int initialCapacity = 1024;
            StringWriter writer = new StringWriter(initialCapacity);
            
            // Write parser error message.
            writer.write("\n******* Input Parameter Error *******\n");
            writer.write(e.getMessage());
            writer.write("\n\n");
            
            // Write usage information--unfortunately we need an output stream.
            writer.write("CreateTenant [options...]\n");
            ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
            parser.printUsage(ostream);
            try {writer.write(ostream.toString("UTF-8"));}
              catch (Exception e1) {}
            writer.write("\n");
            
            // Throw exception.
            throw new TapisException(writer.toString());
           }
        }
      
      // Display help and exit program.
      if (help)
        {
         String s = "\nCreateTenant for creating a new tenant and its default submission queue.";
         System.out.println(s);
         System.out.println("\nCreateTenant [options...]\n");
         parser.printUsage(System.out);
         System.exit(0);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateParms:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Check the semantic integrity of the input parameters. 
     * 
     * @throws JobException
     */
    private void validateParms()
     throws TapisException
    {
      // Make sure we have some non-empty value for the database parms.
      if (StringUtils.isBlank(adminDB)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateParms", "adminDB");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(tapisDB)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateParms", "tapisDB");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(dbUser)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateParms", "dbUser");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(dbPwd)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateParms", "dbPwd");
          _log.error(msg);
          throw new TapisException(msg);
      }
      if (StringUtils.isBlank(dbHost)) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validateParms", "dbHost");
          _log.error(msg);
          throw new TapisException(msg);
      }
    }
}
