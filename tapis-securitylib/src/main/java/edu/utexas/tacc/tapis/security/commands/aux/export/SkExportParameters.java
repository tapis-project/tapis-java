package edu.utexas.tacc.tapis.security.commands.aux.export;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** Parse, validate and massage SkExport parameters.  
 *  
 *  Generally, set -skip to avoid non-service created secrets, including user 
 *  and system secrets.  Set -format to get the path/secret output in JSON as
 *  they appear in Vault or, alternatively, with generated names suitable for 
 *  environment variable assignment.  Set -q to only output the json secrets.      
 * 
 *  The default settings are useful for development and manual validation of 
 *  all secrets.  This approach is appropriate for new Tapis installations.
 *  To avoid outputting user-defined secrets in established installations, 
 *  specify:      
 *  
 *      -skip 
 * 
 *  The settings below are appropriate for deploying new Tapis instances where 
 *  only the secrets referenced in docker compose or kubernetes yml files are 
 *  needed.  Summary information is excluded and output is in environment
 *  variable format (key=value) with the key derived from the Vault path and
 *  sanitized.
 *  
 *      -skip -q -format=ENV
 *      
 * @author rcardone
 */
public class SkExportParameters 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkExportParameters.class);
    
    // The JSON format yields a list of json objects containing "key" and "value"
    // attributes each assigned strings.  The ENV format yields a list of 
    // "name=value" strings suitable for assigning environment variables.
    public enum OutputFormat {JSON, ENV}
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    @Option(name = "-vtok", required = true, aliases = {"--vaulttoken"}, 
            usage = "Vault token with proper authorization")
    public String vtok;
    
    @Option(name = "-vurl", required = true, aliases = {"--vaulturl"}, 
            usage = "Vault URL including port, ex: http(s)://host:32342")
    public String vurl;
    
    @Option(name = "-format", required = false, aliases = {"--format"}, 
            usage = "JSON writes raw Vault data, ENV writes key=value")
    public OutputFormat format = OutputFormat.ENV;
    
    @Option(name = "-noskip", required = false, aliases = {"--noskipusersecrets"}, 
            usage = "true = skip user secrets (use for new installations)")
    public boolean noSkipUserSecrets = false;
    
    @Option(name = "-nosan", required = false, aliases = {"--nosanitize"}, 
            usage = "don't replace unsupported characters with underscore when -format=ENV")
    public boolean noSanitizeName = false;
    
    @Option(name = "-quote", required = false, aliases = {"--quoteenv"}, 
            usage = "enclose secret values in single quotes when -format=ENV")
    public boolean quoteEnvValues = false;
    
    @Option(name = "-v", required = false, aliases = {"--verbose"}, 
            usage = "output statistics in addtion to secrets")
    public boolean verbose = false;
    
    @Option(name = "-help", aliases = {"--help"}, 
            usage = "display help information")
    public boolean help;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkExportParameters(String[] args) 
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
            writer.write("SkExport [options...]\n");
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
         String s = "\nSkExport for exporting Tapis secrets from Vault.";
         System.out.println(s);
         System.out.println("\nSkExport [options...]\n");
         parser.printUsage(System.out);
         
         // Add a usage blurb.
         s = "\n\nThis utility exports as JSON all Tapis secrets currently in Vault.\n";
         System.out.println(s);
         System.exit(0);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateParms:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Check the semantic integrity of the input parameters. Replace all 
     * placeholder characters with spaces in the name and contactName inputs
     * only.
     * 
     * @throws JobException
     */
    private void validateParms()
     throws TapisException
    {
        // Make sure there's a trailing slash in the url.
        if (!vurl.endsWith("/")) vurl += "/";
    }
}
