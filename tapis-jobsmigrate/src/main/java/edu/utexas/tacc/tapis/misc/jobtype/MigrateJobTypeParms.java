package edu.utexas.tacc.tapis.misc.jobtype;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;
import edu.utexas.tacc.tapis.shareddb.migrate.TapisJDBCMigrateParms;

public class MigrateJobTypeParms 
{
    /* **************************************************************************** */
    /*                                 Constants                                    */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(TapisJDBCMigrateParms.class);
       
    // Default password for the service's runtime, non-admin user (tapis).
    private static final String DFT_TAPIS_USER_PASSWORD = "password"; // change on 1st use  
       
    // Jobs db name.
    private static final String  TAPIS_JOBS_DB_NAME = "tapisjobsdb";
    
    /* **************************************************************************** */
    /*                                  Fields                                      */
    /* **************************************************************************** */
     @Option(name = "-h", required = true, aliases = {"-host"}, 
         metaVar = "<name>", usage = "database host (IP address or DNS name)")
     public String host;

     @Option(name = "-u", required = true, aliases = {"-user"}, 
         metaVar = "<name>", usage = "database admin user name")
     public String username;

     @Option(name = "-pw", required = true, aliases = {"-password"}, 
         metaVar = "<string>", usage = "database admin user's password")
     public String password;

     @Option(name = "-dbmsname", required = false,
         metaVar = "<name>", usage = "db management system name (ex: postgresql)")
     public String dbmsName = "postgresql";

     @Option(name = "-p", required = false, aliases = {"-port"}, 
         metaVar = "<num>", usage = "database port number")
     public int port = 5432;

     @Option(name = "-dbname", required = false,
         metaVar = "<name>", usage = "jobs database name")
     public String dbname = TAPIS_JOBS_DB_NAME;

     @Option(name = "-schema", required = false, 
         metaVar = "<name>", usage = "database schema name")
     public String schema = HikariDSGenerator.TAPIS_SCHEMA_NAME;

     @Option(name = "-help", aliases = {"--help"}, 
         usage = "display help information")
     public boolean help;

     /* **************************************************************************** */
     /*                               Constructors                                   */
     /* **************************************************************************** */
     /** Constructor for command line arguments.
      * 
      * @param args command line arguments.
      */
     public MigrateJobTypeParms(String[] args)
         throws TapisJDBCException
     {
         initializeParms(args);
     }
      
     /* **************************************************************************** */
     /*                               Private Methods                                */
     /* **************************************************************************** */
     /* ---------------------------------------------------------------------------- */
     /* initializeParms:                                                             */
     /* ---------------------------------------------------------------------------- */
     /** Parse the input arguments. */
     protected void initializeParms(String[] args)
         throws TapisJDBCException
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
            writer.write("MigrateJobType [options...]\n");
            ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
            parser.printUsage(ostream);
            try {writer.write(ostream.toString(Charset.defaultCharset().toString()));}
              catch (Exception e1) {}
            writer.write("\n");
            
            // Throw exception.
            throw new TapisJDBCException(writer.toString());
           }
        }
      
      // Display help and exit program.
      if (help)
        {
         String s = "\nMigrateJobType assigns the job_type field after adding the column.";
         System.out.println(s);
         System.out.println("\nMigrateJobType [options...] tablename\n");
         parser.printUsage(System.out);
         System.exit(0);
        }
     }
}
