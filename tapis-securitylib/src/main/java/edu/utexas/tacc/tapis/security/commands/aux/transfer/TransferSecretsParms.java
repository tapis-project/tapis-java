package edu.utexas.tacc.tapis.security.commands.aux.transfer;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public class TransferSecretsParms 
{
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    @Option(name = "-stok", required = true, aliases = {"--srctoken"}, 
            usage = "source Vault token")
    public String stok;
    
    @Option(name = "-ttok", required = true, aliases = {"--tgttoken"}, 
            usage = "target Vault token")
    public String ttok;
    
    @Option(name = "-surl", required = true, aliases = {"--srcurl"}, 
            usage = "source Vault URL (including port)")
    public String surl;
    
    @Option(name = "-turl", required = true, aliases = {"--tgturl"}, 
            usage = "target Vault URL (including port)")
    public String turl;
    
    @Option(name = "-dry", required = true, aliases = {"--dryrun"}, 
            usage = "read source secrets but don't write to target")
    public boolean dryRun = false;
    
    @Option(name = "-help", aliases = {"--help"}, 
            usage = "display help information")
    public boolean help;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public TransferSecretsParms(String[] args) 
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
            writer.write("TransferSecrets [options...]\n");
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
         String s = "\nTransferSecrets for transfering secret between Vaults.";
         System.out.println(s);
         System.out.println("\nTransferSecrets [options...]\n");
         parser.printUsage(System.out);
         
         // Add a usage blurb.
         s = "\n\nProvide a token authorized to walk the secrets tree and read each " +
             "\nsecret source in the source Vault.  Provide a token authorized to write " +
             "\nsecrets to the target Vault." +
             "\n\nProvide the base URLs for the source and target Vaults.  The urls must " + 
             "\ninclude the protocol, host and port, but no more.  An example would be:" +
             "\n\n      https://tapis-vault-stage.tacc.utexas.edu:8200\n";
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
        // Canonicalize url inputs.
        if (StringUtils.isBlank(surl)) 
            throw new RuntimeException("Missing required argument: -surl");
        else if (!surl.endsWith("/")) surl += "/";
        if (StringUtils.isBlank(turl)) 
            throw new RuntimeException("Missing required argument: -turl");
        else if (!turl.endsWith("/")) turl += "/";
    }
}
