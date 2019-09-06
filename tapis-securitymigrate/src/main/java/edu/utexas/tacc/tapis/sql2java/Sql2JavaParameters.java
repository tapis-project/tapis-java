package edu.utexas.tacc.tapis.sql2java;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Sql2JavaParameters 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(Sql2JavaParameters.class);
  
  // Default subdirectory used when no output directory is specified.
  private static final String DEFAULT_SUBDIR = "/sql2java";
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  @Option(name = "-h", required = false, aliases = {"-host"}, 
      metaVar = "<name>", usage = "database host (IP address or DNS name)")
  public String host = "localhost";

  @Option(name = "-p", required = false, aliases = {"-port"}, 
      metaVar = "<num>", usage = "database port number")
  public int port = 5432;

  @Option(name = "-u", required = false, aliases = {"-user"}, 
      metaVar = "<name>", usage = "database user name")
  public String username = "tapis";

  @Option(name = "-pw", required = false, aliases = {"-password"}, 
      metaVar = "<string>", usage = "database user's password")
  public String password = "password";

  @Option(name = "-db", required = false, aliases = {"-dbname"},
          metaVar = "<name>", usage = "database name")
      public String dbName = "tapissecdb";

  @Option(name = "-dbmsname", required = false,
      metaVar = "<name>", usage = "db management system name (ex: postgresql)")
  public String dbmsName = "postgresql";
  
  // List of table names that will be put into "NOT LIKE" subclauses in generated 
  // SQL WHERE clause.  The SQL wildcard (%) can be used.
  @Option(name = "-exclude", required = false, aliases = {"-excludetbl"},
      metaVar = "<list>", usage = "comma separated list of tables to exclude")
  public String excludeTables = "flyway_schema_history, %_audit";

  @Option(name = "-o", required = false, aliases = {"-out, -outdir"}, 
      metaVar = "<output file>", usage = "Generated code output directory [$HOME/sql2java]")
  public String outDir;

  @Option(name = "-help", aliases = {"--help"}, 
      usage = "display help information")
  public boolean help;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  public Sql2JavaParameters(String[] args) 
   throws Exception
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
      throws Exception
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
          writer.write("Sql2Java [options...]\n");
          ByteArrayOutputStream ostream = new ByteArrayOutputStream(initialCapacity);
          parser.printUsage(ostream);
          try {writer.write(ostream.toString(Charset.defaultCharset().toString()));}
            catch (Exception e1) {}
          writer.write("\n");
          
          // Throw exception.
          throw new Exception(writer.toString());
         }
      }
    
    // Display help and exit program.
    if (help)
      {
       String s = "\nSql2Java generates Java classes from MySQL schema.";
       System.out.println(s);
       System.out.println("\nSql2Java [options...]\n");
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
   throws IllegalArgumentException
  {
     // Make sure we have an output directory.
     if (StringUtils.isBlank(outDir)) {
         outDir = System.getProperty("user.home") + DEFAULT_SUBDIR;
         File f = new File(outDir);
         f.mkdirs();
     }
     
     // Make sure we can write the output directory.
     File out = new File(outDir);
     if (!out.isDirectory() || !out.canWrite()) {
         String msg = "Unable to write output directory " + out.getAbsolutePath() + ".";
         _log.error(msg);
         throw new IllegalArgumentException(msg);
     }
     
  }
}
