package edu.utexas.tacc.tapis.sharedapi.utils;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class CreateJWTParameters 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(CreateJWTParameters.class);
  
  // Default key name in keystore.s
  private static final String DEFAULT_KEY_ALIAS = "wso2";
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  @Option(name = "-i", required = true, aliases = {"-in, -infile"}, 
      metaVar = "<input file>", usage = "json file containing jwt claims")
  public String inFilename;
  
  @Option(name = "-k", required = false, aliases = {"-keystorefile"},
		  metaVar = "<keystore file>", usage = "keystore file containing the file")
  public String keyStorefile;
  
  @Option(name = "-p", required = true, aliases = {"-password, -pwd"}, 
      metaVar = "<key store password>", usage = "password protecting key store")
  public String password;

  @Option(name = "-o", required = false, aliases = {"-out, -outfile"}, 
      metaVar = "<output file>", usage = "JWT output file [stdout]")
  public String outFilename;

  @Option(name = "-a", required = false, aliases = {"-alias"}, 
     metaVar = "<key alias>", usage = "name by which key is known")
  public String alias = DEFAULT_KEY_ALIAS;
      
  @Option(name = "-help", aliases = {"--help"}, 
      usage = "display help information")
  public boolean help;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  public CreateJWTParameters(String[] args) 
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
          writer.write("CreateJWT [options...]\n");
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
       String s = "\nCreateJWT creates a signed JWT from JSON input.";
       System.out.println(s);
       System.out.println("\nCreateJWT [options...]\n");
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
  }
  
}
