package edu.utexas.tacc.tapis.sharedapi.utils;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class CreateKeyParameters 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(CreateKeyParameters.class);
  
  // Acceptable password lengths.
  private static final int MIN_PASSWORD_LEN = 16;
  private static final int MAX_PASSWORD_LEN = 32;
  
  // Key information.
  private static final int MIN_RSA_KEYSIZE = 2048;
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  @Option(name = "-a", required = true, aliases = {"-alias"}, 
      metaVar = "<key alias>", usage = "name by which key is known")
  public String alias;
  
  @Option(name = "-k", required = false, aliases = {"-keystorefile"},
		  metaVar = "<keystore file>", usage = "keystore file containing the file")
  public String keyStorefile;
  
  @Option(name = "-p", required = true, aliases = {"-password, -pwd"}, 
      metaVar = "<key store password>", usage = "password protecting key store")
  public String password;

  @Option(name = "-u", required = true, aliases = {"-user"}, 
      metaVar = "<user name>", usage = "First and last name of user")
  public String user;

  @Option(name = "-keysize", required = false,  
          metaVar = "<RSA key size>", usage = "2048 or more")
  public int keySize = MIN_RSA_KEYSIZE;

  @Option(name = "-help", aliases = {"--help"}, 
      usage = "display help information")
  public boolean help;
  
  /* ********************************************************************** */
  /*                              Constructors                              */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* constructor:                                                           */
  /* ---------------------------------------------------------------------- */
  public CreateKeyParameters(String[] args) 
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
          writer.write("CreateKey [options...]\n");
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
       String s = "\nCreateKey writes a new public/private key pair into a keystore.";
       System.out.println(s);
       System.out.println("\nCreateKey [options...]\n");
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
      // Minimal password testing.
      if ((password.length() < MIN_PASSWORD_LEN) || (password.length() > MAX_PASSWORD_LEN)) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER_LEN", "CreateKey", "password", 
                                       MIN_PASSWORD_LEN, MAX_PASSWORD_LEN);
          _log.error(msg);
          throw new IllegalArgumentException(msg);
      }
      
      // Key size.
      if (keySize < MIN_RSA_KEYSIZE) {
          String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER_LEN", "CreateKey", "keysize", 
                                       MIN_RSA_KEYSIZE, 4096);
          _log.error(msg);
          throw new IllegalArgumentException(msg);
      }
  }
}
