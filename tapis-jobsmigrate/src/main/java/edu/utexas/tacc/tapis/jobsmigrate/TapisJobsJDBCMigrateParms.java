package edu.utexas.tacc.tapis.jobsmigrate;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import edu.utexas.tacc.tapis.shared.exceptions.TapisJDBCException;
import edu.utexas.tacc.tapis.shareddb.datasource.HikariDSGenerator;
import edu.utexas.tacc.tapis.shareddb.migrate.TapisJDBCMigrateParms;;

/** This is a simple container class for parsed input values. It overrides the
 * some of the values in the superclass
 * 
 * @author rich
 */
public final class TapisJobsJDBCMigrateParms
 extends TapisJDBCMigrateParms
{
 /* **************************************************************************** */
 /*                               Constructors                                   */
 /* **************************************************************************** */
 /** Constructor for command line arguments.
  * 
  * @param args command line arguments.
  */
 public TapisJobsJDBCMigrateParms(String[] args)
     throws TapisJDBCException
 {
  super(args);
 }
  
 /* **************************************************************************** */
 /*                               Private Methods                                */
 /* **************************************************************************** */
 /* ---------------------------------------------------------------------------- */
 /* initializeParms:                                                             */
 /* ---------------------------------------------------------------------------- */
 /** Parse the input arguments. */
 @Override
 protected void initializeParms(String[] args)
     throws TapisJDBCException
 {
  // Override field default values set in superclass after 
  // construction but before we process user input.
  cmdDirectory = "edu/utexas/tacc/tapis/jobsmigrate/sql,classpath:edu/utexas/tacc/tapis/jobsmigrate/scripts";
  schema = HikariDSGenerator.TAPIS_SEC_SCHEMA_NAME;

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
        writer.write("TapisJobsJDBCMigrate [options...]\n");
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
     String s = "\nTapisJobsJDBCMigrate creates and/or migrates databases.";
     System.out.println(s);
     System.out.println("\nTapisJobsJDBCMigrate [options...] tablename\n");
     parser.printUsage(System.out);
     System.exit(0);
    }
 }
}
