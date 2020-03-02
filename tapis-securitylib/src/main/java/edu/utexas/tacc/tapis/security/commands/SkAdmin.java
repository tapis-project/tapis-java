package edu.utexas.tacc.tapis.security.commands;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.util.Base64;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.commands.model.SkAdminSecrets;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminSecretsWrapper;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisJSONException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.schema.JsonValidator;
import edu.utexas.tacc.tapis.shared.schema.JsonValidatorSpec;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

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
    public static final String GENERATE_SECRET = "<generate>";
    
    // Key generation parameters.
    private static final String DFT_KEY_ALGORITHM = "RSA";
    private static final int    DFT_KEY_SIZE      = 4096; // 2048 is the other option
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final SkAdminParameters _parms;
    
    // The secrets input.
    private SkAdminSecrets _secrets;
    
    // Reusable random number generator.
    private SecureRandom _rand;
    
    // Totals.
    private int _secretsCreated;
    private int _secretsUpdated;
    private int _secretsDeployed;
    
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
        
        // Load the input file into a pojo.
        _secrets = loadSecrets();
        
        // Validate the secrets and continue if there
        // were no problems reported.
        if (!validateSecrets()) {
            System.err.println("No changes occurred.");
            return;
        }
        
        // Create or update the secrets.
        createOrUpdateSecrets();
        
        // Deploy the secrets to kubernetes.
        deploySecrets();
        
        // Print results.
        printResults();
    }
    
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
            String msg = MsgUtils.getMsg("SK_ADMIN_NO_SECRETS", _parms.jsonFile);
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
                                                 "secret", secret.service,
                                                 secret.dbhost, secret.dbname,
                                                 secret.dbuser, secret.secretName);
                    _log.error(msg);
                    return false;
                }
                
                // Do we need to generate a password?
                if (GENERATE_SECRET.equals(secret.secret)) 
                    secret.secret = generatePassword();
            }
        
            // Kube changes.
            if (_parms.deploy) {
                
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_DBCRED_MISSING_PARM", 
                                                 "kubeSecretName", secret.service,
                                                 secret.dbhost, secret.dbname,
                                                 secret.dbuser, secret.secretName);
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
            
                if (StringUtils.isBlank(secret.secret)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_JWTSIGNING_MISSING_PARM", 
                                                 "secret", secret.tenant, 
                                                 secret.secretName);
                    _log.error(msg);
                    return false;
                }
                
                // Do we need to generate a password?
                if (GENERATE_SECRET.equals(secret.secret)) 
                    secret.secret = generatePassword();
            }
        
            // Kube changes.
            if (_parms.deploy) {
            
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_JWTSIGNING_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
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
            if (_parms.deploy) {
            
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_SERVICEPWD_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
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
            if (_parms.deploy) {
            
                if (StringUtils.isBlank(secret.kubeSecretName)) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_USER_MISSING_PARM", 
                                                 "kubeSecretName", secret.tenant, 
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
    /* createOrUpdateSecrets:                                                 */
    /* ---------------------------------------------------------------------- */
    private void createOrUpdateSecrets()
    {
        // Are secret changes being requested?
        if (!_parms.create && !_parms.update) return;
        
        // 
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploySecrets:                                                         */
    /* ---------------------------------------------------------------------- */
    private void deploySecrets()
    {
        // Are kubernetes changes being requested?
        if (!_parms.deploy) return;
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* printResults:                                                          */
    /* ---------------------------------------------------------------------- */
    private void printResults()
    {
        
    }
    
    /* ---------------------------------------------------------------------- */
    /* loadSecrets:                                                           */
    /* ---------------------------------------------------------------------- */
    private SkAdminSecrets loadSecrets() throws TapisException
    {
        // Open the input file.
        File file = new File(_parms.jsonFile);
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
        return Base64.getEncoder().encodeToString(bytes);
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
        // Initialize the generator if necessary.
        if (_rand == null)
            // Get the strong random number generator or, 
            // if that fails, the default generator.
            try {_rand = SecureRandom.getInstanceStrong();}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("SK_ADMIN_STRONG_RAND_WARN", e.getMessage());            
                    _log.warn(msg, e);
                    
                    // Use the default generator.
                    _rand = new SecureRandom();
                }
            
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
            var keyPair = gen.genKeyPair();
//            byte[] prvBytes = keyPair.getPrivate().getEncoded();
//            byte[] pubBytes = keyPair.getPublic().getEncoded();
            return keyPair;
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("SK_ADMIN_KEY_GEN_ERROR", DFT_KEY_ALGORITHM,
                                         DFT_KEY_SIZE, e.getMessage());
            throw new TapisException(msg, e);
        }
    }
}