package edu.utexas.tacc.tapis.security.commands;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** Parse, validate and massage SkAdmin parameters.  
 * 
 * @author rcardone
 */
public class SkAdminParameters 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminParameters.class);
    
    // Database defaults.
    private static final String DFT_BASE_SK_URL = "http:/localhost:8080/v3";
    
    // Default name of kubernetes secret that will contain the public key portion of
    // JWT signing keys.  The kubernetes secret contains one public key for each
    // tenant.  By convention, each tenant's entry's name is <tenant>-publickey and 
    // its value is the public key in PEM format.
    private static final String PUBLIC_JWT_SIGNING_KUBE_SECRET_NAME = "tapis-tenants-publickeys";
    
    // Secret generation defaults.
    private static final int DFT_PASSWORD_BYTES = 32;
    private static final int MIN_PASSWORD_BYTES = 16;
    
    // Output choices.
    public static final String OUTPUT_TEXT = "text";
    public static final String OUTPUT_JSON = "json";
    public static final String OUTPUT_YAML = "yaml";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // --------- Action Parameters ---------
    @Option(name = "-c", required = false, aliases = {"-create"}, 
            usage = "create secrets that don't already exist",
            forbids={"-u"})
    public boolean create;
    
    @Option(name = "-u", required = false, aliases = {"-update"}, 
            usage = "create new secrets and update existing ones",
            forbids={"-c"})
    public boolean update;
    
    @Option(name = "-dm", required = false, aliases = {"-deployMerge"}, 
            usage = "deploy secrets to kubernetes, merge with existing",
            forbids={"-dr"}, depends={"-kt","-ku","-kn"})
    public boolean deployMerge;
    
    @Option(name = "-dr", required = false, aliases = {"-deployReplace"}, 
            usage = "deploy secrets to kubernetes, replace any existing",
            forbids={"-dm"}, depends={"-kt","-ku","-kn"})
    public boolean deployReplace;
    
    // --------- Required Parameters -------
    @Option(name = "-i", required = true, aliases = {"-input"}, 
            metaVar = "<file path>", usage = "the json input file or folder")
    public String jsonInput;
    
    @Option(name = "-j", required = true, aliases = {"-jwtenv"}, 
            usage = "JWT environment variable name")
    public String jwtEnv;
    
    // --------- Kube Parameters -----------
    @Option(name = "-kt", required = false, aliases = {"-kubeToken"}, 
            usage = "kubernetes access token environment variable name")
    public String kubeTokenEnv;
    
    @Option(name = "-ku", required = false, aliases = {"-kubeUrl"}, 
            usage = "kubernetes API server URL")
    public String kubeUrl;
    
    @Option(name = "-kn", required = false, aliases = {"-kubeNS"}, 
            usage = "kubernetes namespace to be accessed")
    public String kubeNS;
    
    @Option(name = "-kssl", required = false, 
            usage = "validate SSL connection to kubernetes")
    public boolean kubeValidateSSL = false;
    
    @Option(name = "-kjwtpublickey", required = false, 
            usage = "kubernetes secret that holds JWT public signing keys")
    public String kubeJWTSigningPublicKeySecret = PUBLIC_JWT_SIGNING_KUBE_SECRET_NAME;
    
    // --------- SK Parameters -------------
    @Option(name = "-b", required = false, aliases = {"-baseurl"}, 
            metaVar = "<base sk url>", usage = "SK base url (scheme://host/v3)")
    public String baseUrl = DFT_BASE_SK_URL;
    
    // --------- General Parameters --------
    @Option(name = "-passwordlen", required = false,  
            usage = "number of random bytes in generated passwords")
    public int passwordLength = DFT_PASSWORD_BYTES;
    
    @Option(name = "-o", required = false, aliases = {"-output"}, 
            usage = "'text' (default), 'json' or 'yaml'")
    public String output = OUTPUT_TEXT;
    
    @Option(name = "-help", aliases = {"--help"}, 
            usage = "display help information")
    public boolean help;
    
    // --------- Derived Parameters --------
    // The JWT content read from the jwtEnv environment variable.
    public String jwt;
    
    // The kubernetes access token content read from the kubeTokenEnv environment variable.
    public String kubeToken;
        
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdminParameters(String[] args) 
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
            writer.write("SkAdmin [options...]\n");
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
         String s = "\nSkAdmin for creating and deploying secret to Kubernetes.";
         System.out.println(s);
         System.out.println("\nSkAdmin [options...]\n");
         parser.printUsage(System.out);
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
        // We need to perform some action.
        if (!(create || update || deployMerge || deployReplace)) {
            String msg = "At least one of the following action parameters must be "
                         + "specified: -create, -update, -deployMerge, -deployReplace.";
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Make sure password length exceeds minimum.
        if (passwordLength < MIN_PASSWORD_BYTES) {
            String msg = "The minumum password length is " + MIN_PASSWORD_BYTES + ".";
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Read the JWT into memory.
        jwt = System.getenv(jwtEnv);
        if (StringUtils.isBlank(jwt)) {
            String msg = "Unable to read a JWT from environment variable " + jwtEnv + ".";
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Make sure we have a kubernetes token if we need it.
        if (deployMerge || deployReplace) 
        {
            // Make sure all kube parameters are present.
            if (StringUtils.isBlank(kubeNS)) {
                String msg = "A Kubernetes namespace is required when deploying to Kubernetes.";
                _log.error(msg);
                throw new TapisException(msg);
            }
            if (StringUtils.isBlank(kubeUrl)) {
                String msg = "A Kubernetes server URL is required when deploying to Kubernetes.";
                _log.error(msg);
                throw new TapisException(msg);
            }
            if (StringUtils.isBlank(kubeTokenEnv)) {
                String msg = "A Kubernetes token environment variable is required when deploying to Kubernetes.";
                _log.error(msg);
                throw new TapisException(msg);
            }
            if (StringUtils.isBlank(kubeJWTSigningPublicKeySecret)) {
                String msg = "A Kubernetes public key secret name is required when deploying to Kubernetes.";
                _log.error(msg);
                throw new TapisException(msg);
            }
            
            // Get the kube token.
            kubeToken = System.getenv(kubeTokenEnv);
            if (StringUtils.isBlank(kubeToken)) {
                String msg = "A Kubernetes token is required when deploying to Kubernetes.";
                _log.error(msg);
                throw new TapisException(msg);
            }
        }
        
        // Set the output correctly.
        if (!output.equals(OUTPUT_TEXT) && 
            !output.equals(OUTPUT_JSON) && 
            !output.equals(OUTPUT_YAML)) 
           output = OUTPUT_TEXT;
    }
}
