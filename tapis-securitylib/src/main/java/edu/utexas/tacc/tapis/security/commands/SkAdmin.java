package edu.utexas.tacc.tapis.security.commands;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminResults;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminSecrets;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminSecretsWrapper;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminDBCredentialProcessor;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminJwtSigningProcessor;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminKubeDeployer;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminServicePwdProcessor;
import edu.utexas.tacc.tapis.security.commands.processors.SkAdminUserProcessor;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

/** The utility class create or update secrets in SK and, optionally, deploy those
 * secrets in Kubernetes. The SkAdminParameters class defines all possible parameters
 * to this program; see that class for details.  
 * 
 * Actions
 * -------
 * Each of the four action parameters control what this program does and can be 
 * described as follows:
 * 
 *  1. create - creates a secret in SK only if the secret doesn't already exist.
 *  2. update - updates an existing secret in SK or creating it if it doesn't exist.
 *  3. deployMerge - deploy the specified key/value pairs into secrets, preserving any 
 *                   key/value pairs already in the secrets that don't conflict the
 *                   specified ones.
 *  4. deployReplace - deploy the specified key/value pairs of secrets, completely
 *                   overwriting any previously existing key/value pairs.
 *                   
 * An execution of this program can affect SK secrets stored in Vault, Kubernetes
 * secrets, or both.
 * 
 * JSON Input
 * ----------
 * All input is in the form of one or more json files that conform to the json schema
 * defined in the SkAdminInput.json resource file.  Each secret type that can be 
 * deployed to Kubernetes is represented.  These SK secret types are defined in the
 * SecretType enum class and include DBCredential, JWTSigning, ServicePwd and User; 
 * the System type is excluded because system secrets are never deployed to Kubernetes.
 * 
 * The -i (-input) parameter can be assigned a single json file or a directory that
 * contains json files.  When a directory is specified, all json files that are 
 * immediate children of the directory are loaded and merged into a single set of
 * secrets to be processed.  Subdirectories are ignored.  This merging allows 
 * smaller, easier to manage, input files to be used.  For example, input files
 * can be defined to contain all the key/value pairs that will be deployed under a 
 * single Kubernetes secret, which could include different types of SK secrets. 
 * 
 * Password and Key Input
 * ----------------------
 * On input, the values of passwords and keys fields can be specified using the 
 * distinguished value "<generate-secret>".  SkAdmin will generate random passwords
 * or asymmetric key pairs as required.
 * 
 * The value of keys fields can also be specified as "file:pathToPEMFile" where
 * the pathToPEMFile is a path, usually an absolute path, to a PEM file containing 
 * a public or private key.
 * 
 * When key values are provided inline in the input json files, the values are
 * required to be in PEM format.
 * 
 * Result Reporting
 * ----------------
 * An accounting of what action were performed is printed to standard out when the
 * program completes.  Summary information include counts of secret processing.
 * Detailed information includes an outcome message for each secret action that
 * includes success, failure and skipped outcomes.
 * 
 * The default result format is text, but json and yaml can also be specified.
 * 
 * @author rcardone
 */
public class SkAdmin 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdmin.class);
    
    // The input schema definition.
    private static final String FILE_SKADMIN_INPUT_SCHEMA = 
        "/edu/utexas/tacc/tapis/security/jsonschema/SkAdminInput.json";
    
    // The distinguished string that causes secrets to be generated.    
    public static final String GENERATE_SECRET = "<generate-secret>";
    public static final String READ_FILE       = "file:";
    
    // Key generation parameters.
    private static final String DFT_KEY_ALGORITHM = "RSA";
    private static final int    DFT_KEY_SIZE      = 2048; // 4096 is the other option
    
    // PEM text for public and private keys.
    private static final String PUB_KEY_PROLOGUE = "-----BEGIN PUBLIC KEY-----\n";
    private static final String PUB_KEY_EPILOGUE = "\n-----END PUBLIC KEY-----";
    private static final String PRV_KEY_PROLOGUE = "-----BEGIN PRIVATE KEY-----\n";
    private static final String PRV_KEY_EPILOGUE = "\n-----END PRIVATE KEY-----";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final SkAdminParameters _parms;
    
    // The secrets input.
    private SkAdminSecrets _secrets;
    
    // The kubernetes secrets map used only for deployment.  The key is the
    // kubernetes secret name, the value a map of key names to unencoded secrets.
    private final HashMap<String,HashMap<String,String>> _kubeSecretMap = new HashMap<>();
    
    // The deployment recorder inner class writes to the _kubeSecretMap;
    private DeployRecorder _deployRecorder;
    
    // Reusable random number generator.
    private SecureRandom _rand;
    
    // Secret processors.
    private SkAdminDBCredentialProcessor _dbCredentialProcessor;
    private SkAdminJwtSigningProcessor   _jwtSigningProcessor;
    private SkAdminServicePwdProcessor   _servicePwdProcessor;
    private SkAdminUserProcessor         _userProcessor;
    
    // Create the singleton instance for use throughout.
    protected final SkAdminResults _results = SkAdminResults.getInstance();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SkAdmin(SkAdminParameters parms) 
    {
        // Parameters cannot be null.
        if (parms == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "SkAdmin", "parms");
          _log.error(msg);
          throw new IllegalArgumentException(msg);
        }
        _parms = parms;
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    /** No logging necessary in this method since the called methods log errors.
     * 
     * @param args the command line parameters
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception 
    {
        // Parse the command line parameters.
        SkAdminParameters parms = null;
        parms = new SkAdminParameters(args);
        
        // Start the worker.
        SkAdmin skAdmin = new SkAdmin(parms);
        skAdmin.admin();
    }

    /* ---------------------------------------------------------------------- */
    /* admin:                                                                 */
    /* ---------------------------------------------------------------------- */
    public void admin()
     throws TapisException
    {
        //var keyPair = generateKeyPair();
        
        // Load the input file or files into a pojo.
        _secrets = loadSecrets();
        
        // Validate the secrets and continue if there
        // were no problems reported.
        if (!validateSecrets()) {
            System.err.println("No changes occurred.");
            return;
        }
        
        // Create secret processors. Runtime exceptions 
        // can be thrown from here.
        createProcessors();
        
        // Note: At most only one of create or update can be set.
        //
        // Create the secrets.
        createSecrets();
        
        // Update the secrets.
        updateSecrets();
        
        // Deploy the secrets to kubernetes.
        deploySecrets();
        
        // Disconnect processors.
        disconnectProcessors();
        
        // Print results.
        printResults();
    }
    
    /* ---------------------------------------------------------------------- */
    /* createsJwtSigningPublicKeyMapName:                                     */
    /* ---------------------------------------------------------------------- */
    /** Generate a string to be used as the key in the generated public key map.
     * 
     * @param tenant the jwtSigningPwd tenant
     * @param secretName the jwtSigningPwd secretName
     * @return the string to be used as a key in the generated public key map
     */
    public static String createsJwtSigningPublicKeyMapName(String tenant, String secretName)
    {return secretName + '@' + tenant;}
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* validateSecrets:                                                       */
    /* ---------------------------------------------------------------------- */
    private boolean validateSecrets()
    {
        // Make sure the secrets object was created.
        if (_secrets == null) {
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRETS", _parms.jsonInput);
            _log.error(msg);
            return false;
        }
        
        // Validate each type of secret. Each method writes 
        // its own messages out when errors are encountered.
        if (!validateDBCredential()) return false;
        if (!validateJwtSigning()) return false;
        if (!validateServicePwd()) return false;
        if (!validateUser()) return false;
        
        // We're good.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateDBCredential:                                                  */
    /* ---------------------------------------------------------------------- */
    private boolean validateDBCredential()
    {
        // Maybe there's nothing to do.
        if (_secrets.dbcredential == null || _secrets.dbcredential.isEmpty())
            return true;
        
        // Iterate throught the secrets.
        for (var secret : _secrets.dbcredential) {
            // Vault changes.
            if (_parms.create || _parms.update) {
                
                if (StringUtils.isBlank(secret.secret)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_DBCRED_MISSING_PARM", 
                                                 "secret", secret.dbservice,
                                                 secret.dbhost, secret.dbname,
                                                 secret.user, secret.secretName);
                    _log.error(msg);
                    return false;
                }
                
                // Do we need to generate a password?
                if (GENERATE_SECRET.equals(secret.secret)) 
                    secret.secret = generatePassword();
            }
        
            // Kube changes.
            if (_parms.deployMerge || _parms.deployReplace) {
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_DBCRED_MISSING_PARM", 
                                                 "kubeSecretName", secret.dbservice,
                                                 secret.dbhost, secret.dbname,
                                                 secret.user, secret.secretName);
                    _log.error(msg);
                    return false;
                }
                if (StringUtils.isBlank(secret.kubeSecretKey)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_DBCRED_MISSING_PARM", 
                                                 "kubeSecretKey", secret.dbservice,
                                                 secret.dbhost, secret.dbname,
                                                 secret.user, secret.secretName);
                    _log.error(msg);
                    return false;
                }
            }
        }
        
        // We're good.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateJwtSigning:                                                    */
    /* ---------------------------------------------------------------------- */
    private boolean validateJwtSigning()
    {
        // Maybe there's nothing to do.
        if (_secrets.jwtsigning == null || _secrets.jwtsigning.isEmpty())
            return true;
        
        // Iterate throught the secrets.
        for (var secret : _secrets.jwtsigning) {
            // Vault changes.
            if (_parms.create || _parms.update) {
            
                // At a minimum, the private key must be specified.
                if (StringUtils.isBlank(secret.privateKey)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_JWTSIGNING_MISSING_PARM", 
                                                 "privateKey", secret.tenant, 
                                                 secret.secretName);
                    _log.error(msg);
                    return false;
                }
                
                // Do we need to generate a key pair?
                if (GENERATE_SECRET.equals(secret.privateKey)) {
                    // Create new keys.
                    KeyPair keyPair = null;
                    try {keyPair = generateKeyPair();}
                        catch (Exception e) {
                            String msg = MsgUtils.getMsg("SK_ADMIN_SIGNING_KEY_GEN_ERROR", 
                                                         secret.tenant, secret.secretName);
                            _log.error(msg);
                            return false;
                        }
                    
                    // Save the private key in PEM format in the user designated place.
                    secret.privateKey = generatePrivatePemKey(keyPair.getPrivate());
                    secret.publicKey = generatePublicPemKey(keyPair.getPublic());
                } 
                else {
                    // Read the private key from a PEM file.
                    if (secret.privateKey.startsWith(READ_FILE)) {
                        // Get the path.
                        String path = null;
                        if (secret.privateKey.length() > READ_FILE.length())
                            path = secret.privateKey.substring(READ_FILE.length());
                        if (StringUtils.isBlank(path)) {
                            String msg = MsgUtils.getMsg("SK_ADMIN_SIGNING_KEY_FILE_MISSING", 
                                                         "private", secret.tenant, secret.secretName);
                            _log.error(msg);
                            return false;
                        }
                        
                        // Read the file into the private key.
                        try {secret.privateKey = Files.readString(Paths.get(path));}
                            catch (Exception e) {
                                String msg = MsgUtils.getMsg("SK_ADMIN_SIGNING_KEY_FILE_ERROR", 
                                              path, secret.tenant, secret.secretName, e.getMessage());
                                _log.error(msg, e);
                                return false;
                            }
                    }
                    
                    // Read the public key PEM file if one is provided.
                    if (secret.publicKey != null && secret.publicKey.startsWith(READ_FILE)) {
                        // Get the path.
                        String path = null;
                        if (secret.publicKey.length() > READ_FILE.length())
                            path = secret.publicKey.substring(READ_FILE.length());
                        if (StringUtils.isBlank(path)) {
                            String msg = MsgUtils.getMsg("SK_ADMIN_SIGNING_KEY_FILE_MISSING", 
                                                         "public", secret.tenant, secret.secretName);
                            _log.error(msg);
                            return false;
                        }
                        
                        // Read the file into the private key.
                        try {secret.publicKey = Files.readString(Paths.get(path));}
                            catch (Exception e) {
                                String msg = MsgUtils.getMsg("SK_ADMIN_SIGNING_KEY_FILE_ERROR", 
                                              path, secret.tenant, secret.secretName, e.getMessage());
                                _log.error(msg, e);
                                return false;
                            }
                    }
                }
            }
        
            // Kube changes.
            if (_parms.deployMerge || _parms.deployReplace) {
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_JWTSIGNING_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
                                                 secret.secretName);
                    _log.error(msg);
                    return false;
                }
                if (StringUtils.isBlank(secret.kubeSecretKey)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_JWTSIGNING_MISSING_PARM", 
                                                 "kubeSecretKey", secret.tenant, 
                                                 secret.secretName);
                    _log.error(msg);
                    return false;
                }
            }
        }
        
        // We're good.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateServicePwd:                                                    */
    /* ---------------------------------------------------------------------- */
    private boolean validateServicePwd()
    {
        // Maybe there's nothing to do.
        if (_secrets.servicepwd == null || _secrets.servicepwd.isEmpty())
            return true;
        
        // Iterate throught the secrets.
        for (var secret : _secrets.servicepwd) {
            // Vault changes.
            if (_parms.create || _parms.update) {
            
                if (StringUtils.isBlank(secret.password)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_SERVICEPWD_MISSING_PARM", 
                                                 "password", secret.tenant, secret.service,
                                                 secret.secretName);
                    _log.error(msg);
                    return false;
                }
                
                // Do we need to generate a password?
                if (GENERATE_SECRET.equals(secret.password)) 
                    secret.password = generatePassword();
            }
        
            // Kube changes.
            if (_parms.deployMerge || _parms.deployReplace) {
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_SERVICEPWD_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
                                                 secret.service, secret.secretName);
                    _log.error(msg);
                    return false;
                }
                if (StringUtils.isBlank(secret.kubeSecretKey)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_SERVICEPWD_MISSING_PARM", 
                                                 "kubeSecretKey", secret.tenant, 
                                                 secret.service, secret.secretName);
                    _log.error(msg);
                    return false;
                }
            }
        }
        
        // We're good.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* validateUser:                                                          */
    /* ---------------------------------------------------------------------- */
    private boolean validateUser()
    {
        // Maybe there's nothing to do.
        if (_secrets.user == null || _secrets.user.isEmpty())
            return true;
        
        // Iterate throught the secrets.
        for (var secret : _secrets.user) {
            // Vault changes.
            if (_parms.create || _parms.update) {
            
                // Empty strings are allowed.
                if (secret.value == null) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_USER_MISSING_PARM", 
                                                 "value", secret.tenant, secret.user,
                                                 secret.key, secret.secretName);
                    _log.error(msg);
                    return false;
                }
            }
        
            // Kube changes.
            if (_parms.deployMerge || _parms.deployReplace) {
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_USER_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
                                                 secret.user, secret.key, secret.secretName);
                    _log.error(msg);
                    return false;
                }
                if (StringUtils.isBlank(secret.kubeSecretKey)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_USER_MISSING_PARM", 
                                                 "kubeSecretKey", secret.tenant, 
                                                 secret.user, secret.key, secret.secretName);
                    _log.error(msg);
                    return false;
                }
            }
        }
        
        // We're good.
        return true;
    }
    
    /* ---------------------------------------------------------------------- */
    /* createProcessors:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Initialize all processors and the shared SKClient.    
     * 
     * @throws TapisRuntimeException, IllegalArgumentException
     */
    private void createProcessors()
    {
        _dbCredentialProcessor = 
            new SkAdminDBCredentialProcessor(_secrets.dbcredential, _parms);
        _jwtSigningProcessor = 
            new SkAdminJwtSigningProcessor(_secrets.jwtsigning, _parms);
        _servicePwdProcessor = 
            new SkAdminServicePwdProcessor(_secrets.servicepwd, _parms);
        _userProcessor = 
            new SkAdminUserProcessor(_secrets.user, _parms);
    }
    
    /* ---------------------------------------------------------------------- */
    /* disconnectProcessors:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Disconnect the shared SKClient.    
     */
    private void disconnectProcessors()
    {
        // Pick any of the processors,
        // only one needs to be called.
        _dbCredentialProcessor.close();
    }
    
    /* ---------------------------------------------------------------------- */
    /* createSecrets:                                                         */
    /* ---------------------------------------------------------------------- */
    private void createSecrets()
    {
        // Are secret changes being requested?
        if (!_parms.create) return;
        
        // Create each type of secret separately.
        _dbCredentialProcessor.create();
        _jwtSigningProcessor.create();
        _servicePwdProcessor.create();
        _userProcessor.create();
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateSecrets:                                                         */
    /* ---------------------------------------------------------------------- */
    private void updateSecrets()
    {
        // Are secret changes being requested?
        if (!_parms.update) return;
        
        // Update each type of secret separately.
        _dbCredentialProcessor.update();
        _jwtSigningProcessor.update();
        _servicePwdProcessor.update();
        _userProcessor.update();
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploySecrets:                                                         */
    /* ---------------------------------------------------------------------- */
    private void deploySecrets()
    {
        // Are kubernetes changes being requested?
        if (!_parms.deployMerge && !_parms.deployReplace) return;
        
        // Create the deployment recorder object.
        _deployRecorder = new DeployRecorder();
        
        // Deploy each type of secret types separately.
        // Deployment here means that the processors
        // use the recorder to insert kube secret info
        // into the _kubeSecretMap.
        _dbCredentialProcessor.deploy(_deployRecorder);
        _jwtSigningProcessor.deploy(_deployRecorder);
        _servicePwdProcessor.deploy(_deployRecorder);
        _userProcessor.deploy(_deployRecorder);
        
        // Deploy to kubernetes.
        SkAdminKubeDeployer deployer = new SkAdminKubeDeployer(_kubeSecretMap, _parms);
        deployer.deploy();
    }
    
    /* ---------------------------------------------------------------------- */
    /* printResults:                                                          */
    /* ---------------------------------------------------------------------- */
    private void printResults()
    {   
        // Json or plain text output to standard out.
        if (_parms.output.equals(SkAdminParameters.OUTPUT_JSON))
            System.out.println(_results.toJson());
        else if (_parms.output.equals(SkAdminParameters.OUTPUT_YAML))
            System.out.println(_results.toYaml());
        else System.out.println(_results.toText());
    }
    
    /* ---------------------------------------------------------------------- */
    /* loadSecrets:                                                           */
    /* ---------------------------------------------------------------------- */
    private SkAdminSecrets loadSecrets() throws TapisException
    {
        // Get the file or directory containing the json input.
        File file = new File(_parms.jsonInput);
        if (file.isFile()) return loadSecretFile(file);
        
        // We're dealing with a directory.  We only process the immediate
        // contents of the directory and ignore subdirectories and files 
        // that don't end with the .json suffix.
        File[] files = file.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(".json")) return true;
                  else return false;
            }});
        if (files == null) {
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRETS", _parms.jsonInput);
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Put files in alphabetic order, ignore directories.
        var map = new TreeMap<String,File>();
        for (File f : files) 
            if (f.isFile()) map.put(f.getAbsolutePath(), f);
        
        // Make sure we have at least one file.
        if (map.isEmpty()) {
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRETS", _parms.jsonInput);
            _log.error(msg);
            throw new TapisException(msg);
        }
        
        // Create the final result object that accumulate each file's result.
        SkAdminSecrets result = new SkAdminSecrets();
        result.dbcredential = new ArrayList<>();
        result.jwtsigning   = new ArrayList<>();
        result.servicepwd   = new ArrayList<>();
        result.user         = new ArrayList<>();
        
        // Read in each file and accumulate its results.
        for (File f : map.values()) {
            var r = loadSecretFile(f);
            if (r.dbcredential != null) result.dbcredential.addAll(r.dbcredential);
            if (r.jwtsigning != null)   result.jwtsigning.addAll(r.jwtsigning);
            if (r.servicepwd != null)   result.servicepwd.addAll(r.servicepwd);
            if (r.user != null)         result.user.addAll(r.user);
        }
        
        return result;
    }
    
    /* ---------------------------------------------------------------------- */
    /* loadSecretFile:                                                        */
    /* ---------------------------------------------------------------------- */
    private SkAdminSecrets loadSecretFile(File file) throws TapisException
    {
        // Open the input file.
        Reader reader;
        try {reader = new FileReader(file, Charset.forName("UTF-8"));}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_FILE_READ_ERROR", file.getAbsolutePath(),
                                             e.getMessage());
                _log.error(msg, e);
                throw new TapisException(msg, e);
            }
        
        // Read the input into a string.
        String json = null;
        try {json = IOUtils.toString(reader);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("NET_INVALID_JSON_INPUT", "security", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }
          finally {
              // Always close the file.
              try {reader.close();} catch (Exception e) {}
          }
        
        // Create validator specification.
        JsonValidatorSpec spec = new JsonValidatorSpec(json, FILE_SKADMIN_INPUT_SCHEMA);
        
        // Make sure the json conforms to the expected schema.
        try {JsonValidator.validate(spec);}
          catch (TapisJSONException e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_VALIDATION_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisException(msg, e);
          }

        // Convert the json into a pojo.
        SkAdminSecretsWrapper wrapper;
        try {wrapper = TapisGsonUtils.getGson().fromJson(json, SkAdminSecretsWrapper.class);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_JSON_PARSE_ERROR", "SkAdmin",
                                         file.getAbsolutePath(), e.getMessage());            
            _log.error(msg, e);
            throw new TapisException(msg, e);
        }

        // Return the wrapper content.
        return wrapper.secrets;
    }
    
    /* ---------------------------------------------------------------------- */
    /* generatePassword:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Generate a random base 64 password. 
     * 
     * @return the password
     */
    private String generatePassword()
    {
        // Generate the random bytes and return the base 64 representation.
        byte[] bytes = new byte[_parms.passwordLength];
        getRand().nextBytes(bytes);
        String password = Base64.getEncoder().encodeToString(bytes);
        _results.incrementPasswordsGenerated();
        return password;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRand:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Return a new random number generator if one hasn't already been 
     * initialized, otherwise return the exising one.
     * 
     * @return the generator
     */
    private SecureRandom getRand()
    {
        // Initialize the generator if necessary.  We use the default generator
        // which should use a secure a seed just like SecureRandom.getInstanceStrong(),
        // but from that point on is deterministic.  The strong one is real slow.
        // Interesting reads:
        //
        //  https://tersesystems.com/blog/2015/12/17/the-right-way-to-use-securerandom/
        //  https://www.2uo.de/myths-about-urandom/
        //
        if (_rand == null) _rand = new SecureRandom();
            
        return _rand;
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateKeyPair:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Generate a key pair.  Use the getEncoded() method on each of the keys
     * to get the binary key values.
     * 
     * @return the public and private keys
     * @throws TapisException on error
     */
    private KeyPair generateKeyPair() throws TapisException 
    {
        try {
            var gen = KeyPairGenerator.getInstance(DFT_KEY_ALGORITHM);
            gen.initialize(DFT_KEY_SIZE);
            _results.incrementKeyPairsGenerated();
            return gen.genKeyPair();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_ADMIN_KEY_GEN_ERROR", DFT_KEY_ALGORITHM,
                                         DFT_KEY_SIZE, e.getMessage());
            throw new TapisException(msg, e);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* generatePublicPemKey:                                                  */
    /* ---------------------------------------------------------------------- */
    private String generatePublicPemKey(PublicKey key)
    {
        // Encode raw bytes to base 64 and add surrounding text.
        var encoder = Base64.getEncoder();
        String pem = encoder.encodeToString(key.getEncoded());
        return PUB_KEY_PROLOGUE + pem + PUB_KEY_EPILOGUE;
    }
    
    /* ---------------------------------------------------------------------- */
    /* generatePrivatePemKey:                                                 */
    /* ---------------------------------------------------------------------- */
    private String generatePrivatePemKey(PrivateKey key)
    {
        // Encode raw bytes to base 64 and add surrounding text.
        var encoder = Base64.getEncoder();
        String pem = encoder.encodeToString(key.getEncoded());
        return PRV_KEY_PROLOGUE + pem + PRV_KEY_EPILOGUE;
    }

    /* ********************************************************************** */
    /*                         Deployment Recorder Class                      */
    /* ********************************************************************** */
    private final class DeployRecorder implements ISkAdminDeployRecorder
    {
        @Override
        public void addDeployRecord(String kubeSecretName, String kubeKey, String base64Secret) 
        {
            // Create the secret entry if it doesn't already exist.
            var map = _kubeSecretMap.get(kubeSecretName);
            if (map == null) {
                map = new HashMap<String,String>();
                _kubeSecretMap.put(kubeSecretName, map);
                
            }
            
            // Add the key/value pair to the secret.
            map.put(kubeKey, base64Secret);
        }
    }
}
