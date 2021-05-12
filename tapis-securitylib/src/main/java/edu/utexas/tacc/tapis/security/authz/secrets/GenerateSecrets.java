package edu.utexas.tacc.tapis.security.authz.secrets;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.Base62;

/** Class used to dynamically generate passwords and public/private key pairs
 * to store into SK.
 * 
 * @author rcardone
 */
public final class GenerateSecrets 
{
    /* ********************************************************************** */
    /*                                Constants                               */
    /* ********************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(GenerateSecrets.class);
    
    // Directive indicating a secret should be generated.
    private static final String GENERATE_SECRET = "<generate-secret>";
    
    // 128 bit passwords.
    private static final int DFT_PASSWORD_BYTES = 16;
    
    // Key generation parameters.
    private static final String DFT_KEY_ALGORITHM = "RSA";
    private static final int    DFT_KEY_SIZE      = 2048; // 4096 is the other option
    
    // PEM text for public and private keys.
    private static final String PUB_KEY_PROLOGUE = "-----BEGIN PUBLIC KEY-----\n";
    private static final String PUB_KEY_EPILOGUE = "\n-----END PUBLIC KEY-----";
    private static final String PRV_KEY_PROLOGUE = "-----BEGIN PRIVATE KEY-----\n";
    private static final String PRV_KEY_EPILOGUE = "\n-----END PRIVATE KEY-----";

    // JWTSigning item names.
    private static final String DEFAULT_PRIVATE_KEY_NAME = "privateKey";
    private static final String DEFAULT_PUBLIC_KEY_NAME  = "publicKey";

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    private final SecretType          _secretType;
    private final String              _secretName;
    private final Map<String, Object> _secretMap;
    
    // Reusable random number generator.
    private SecureRandom              _rand;;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /** Caller guarantees non-null parameters.
     * 
     * @param secretType the type of secret being written
     * @param secretName the user-chosen name of the secret
     * @param secretMap the key/value map of secret items
     */
    public GenerateSecrets(SecretType secretType, String secretName,
                           Map<String, Object> secretMap)
    {
        _secretType = secretType;
        _secretName = secretName;
        _secretMap  = secretMap;
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generate:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Generate passwords and key pairs on demand based on the field values
     * set on construction.  
     * 
     * Key pairs are always generated for secrets of type JWTSigning while 
     * passwords are usually generated for all other secret types.  This default 
     * password generation behavior is overridden when this exact _secretMap 
     * item is encountered:
     * 
     *    key="privateKey", value="<generate-secret>
     *   
     * In this case, a private key pair is generated just like in the JWTSigning
     * case.  When a key pair is generated, the _secretMap item that triggered 
     * the generation is replaced by these two items:
     * 
     *    key="privateKey", value=<private key in pem format>
     *    key="publicKey",  value=<public key in pem format>
     *   
     * Passwords are generated for non-JWTSigning secret types when _secretMap
     * items like this are encountered:
     *   
     *   key=<name other than privateKey>, value="<generate-secret>
     *   
     * The generated password simply replaces the item's value and the key name
     * is left unchanged.  
     * 
     * @throws TapisException
     */
    public void generate() throws TapisException
    {
        // Create a map for new key pairs.
        var newKeysMap = new HashMap<String, Object>();
        
        // Iterate through the secret map to see if any have the 
        // <generate-secret> directive as a value.
        var it = _secretMap.entrySet().iterator();
        while (it.hasNext()) 
        {
            // Look for the generate directive.
            var entry = it.next();
            if (!GENERATE_SECRET.equals(entry.getValue())) continue;
            
            // Run the generation based on secret type or 
            // the name of the entry key.
            if (_secretType == SecretType.JWTSigning ||
                DEFAULT_PRIVATE_KEY_NAME.equals(entry.getKey())) {
                // Create new keys.
                KeyPair keyPair = generateKeyPair();
                
                // Save the private key in PEM format in the user designated place.
                String privateKey = generatePrivatePemKey(keyPair.getPrivate());
                String publicKey  = generatePublicPemKey(keyPair.getPublic());
                
                // Add the new mappings to the auxilary map.  Note that standard 
                // key names will replace any name from the user.
                newKeysMap.put(DEFAULT_PRIVATE_KEY_NAME, privateKey);
                newKeysMap.put(DEFAULT_PUBLIC_KEY_NAME, publicKey);
                
                // Log the key generation since the key name can change.
                if (_log.isInfoEnabled())
                    _log.info(MsgUtils.getMsg("SK_KEYPAIR_GENERATED", _secretType.name(),
                                              _secretName, entry.getKey(), 
                                              GENERATE_SECRET, DEFAULT_PRIVATE_KEY_NAME,
                                              DEFAULT_PUBLIC_KEY_NAME));
                
                // Always delete the original entry.
                it.remove();
            }
            else {
                // In-place update of value, so the key remains the same.
                // Our convention is that the key should be named "password",
                // but it's not enforced here.
                entry.setValue(generatePassword());
                
                // Trace the password generation.
                if (_log.isInfoEnabled())
                    _log.info(MsgUtils.getMsg("SK_PASSWORD_GENERATED", _secretType.name(),
                                              _secretName, entry.getKey(), GENERATE_SECRET));
            }
        }
        
        // Add any new secrets that we created along the way.
        _secretMap.putAll(newKeysMap);
    }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    // Similar code can be found in SkAdmin, but we keep the two codebases
    // separate since that run in distinct environments.
    
    /* ---------------------------------------------------------------------- */
    /* generatePassword:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Generate a random base 62 password. 
     * 
     * @return the password
     */
    private String generatePassword()
    {
        // Generate the random bytes and return the base 62 representation.
        byte[] bytes = new byte[DFT_PASSWORD_BYTES];
        getRand().nextBytes(bytes);
        String password = Base62.base62Encode(bytes);
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
}
