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
    
    // Secret generation defaults.
    private static final int DFT_PASSWORD_BYTES = 16;
    private static final int MIN_PASSWORD_BYTES = 8;
    
    // Output choices.
    public static final String OUTPUT_TEXT = "text";
    public static final String OUTPUT_JSON = "json";
    public static final String OUTPUT_YAML = "yaml";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // --------- Action Parameters ---------
    // At least one of the following 4 parameters must be set.
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
    // There has to be input.
    @Option(name = "-i", required = true, aliases = {"-input"}, 
            metaVar = "<file path>", usage = "the json input file or folder")
    public String jsonInput;
    
    // SK or Vault url.
    @Option(name = "-b", required = true, aliases = {"-baseurl"}, 
            metaVar = "<base sk or vault url>", usage = "SK: http(s)://host/v3, Vault: http(s)://host:32342)")
    public String baseUrl;
    
    // --------- Kube Parameters -----------
    // Kube parameter are necessary if a deploy action is specified.
    @Option(name = "-kt", required = false, aliases = {"-kubeToken"}, 
            usage = "kubernetes access token environment variable name",
            depends={"-ku","-kn"})
    public String kubeTokenEnv;
    
    @Option(name = "-ku", required = false, aliases = {"-kubeUrl"}, 
            usage = "kubernetes API server URL",
                    depends={"-kt","-kn"})
    public String kubeUrl;
    
    @Option(name = "-kn", required = false, aliases = {"-kubeNS"}, 
            usage = "kubernetes namespace to be accessed",
                    depends={"-ku","-kt"})
    public String kubeNS;
    
    @Option(name = "-kssl", required = false, 
            usage = "validate SSL connection to kubernetes")
    public boolean kubeValidateSSL = false;
    
    // --------- SK Parameters -------------
    // SK or Vault parameters are always required, but not both.
    @Option(name = "-j", required = false, aliases = {"-jwtenv"}, 
            usage = "JWT environment variable name",
            forbids={"-vr","-vs"})
    public String jwtEnv;
    
    // --------- Vault Parameters ----------
    // SK or Vault parameters are always required, but not both
    @Option(name = "-vr", required = false, aliases = {"-vaultRole"}, 
            usage = "vault role-id",
            forbids={"-j"}, depends={"-vs"})
    public String vaultRoleId;
    
    @Option(name = "-vs", required = false, aliases = {"-vaultSecret"}, 
            usage = "vault secret-id",
            forbids={"-j"}, depends={"-vr"})
    public String vaultSecretId;
       
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
    /*                               Public Methods                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* useSK:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** Determine if we are using SK or going directly to Vault.
     * 
     * @return true if using SK, false if using Vault
     */
    public boolean useSK() {return jwt != null;}
    
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
         String s = "\nSkAdmin for creating and deploying secrets to Kubernetes.";
         System.out.println(s);
         System.out.println("\nSkAdmin [options...]\n");
         parser.printUsage(System.out);
         
         // Add a usage blurb.
         s = "\n\nUse either the -c or -u option to change secrets in Vault. Use the -dm " +
             "\nor -dr option to deploy secrets to Kubernetes." +
             "\n\nAccess to Vault secrets is always required. Use either the -j option " +
             "\nto access the secrets using the Security Kernel or the {-vr, -vs} options " +
             "\nto accress the secrets by going directly to Vault. Set the baseurl to " +
             "\nmatch the access method.  Set the {-kn, -kt, -ku} options when deploying " +
             "\nsecrets to Kubernetes.\n";
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
        
        // Make sure Vault access is configured either directly or through SK.
        if (StringUtils.isBlank(jwtEnv) && StringUtils.isBlank(vaultRoleId)) {
            String msg = "Either an SK JWT or a Vault role-id/secret-id pair must be provided.";
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Read the JWT into memory.
        if (jwtEnv != null) {
            jwt = System.getenv(jwtEnv);
            if (StringUtils.isBlank(jwt)) {
                String msg = "Unable to read a JWT from environment variable " + jwtEnv + ".";
                _log.error(msg);
                throw new TapisException(msg);
            }
        }
        
        // Make sure we have a kubernetes token if we need it.
        if (deployMerge || deployReplace) 
        {
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
