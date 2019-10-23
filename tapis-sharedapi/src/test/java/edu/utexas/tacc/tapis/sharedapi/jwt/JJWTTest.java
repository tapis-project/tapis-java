package edu.utexas.tacc.tapis.sharedapi.jwt;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.Map.Entry;
import java.util.Base64;
import java.util.Set;
import java.util.Base64.Encoder;

import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.Base64UrlCodec;
import io.jsonwebtoken.impl.crypto.RsaProvider;

/** This class tests a custom version of jjwt. That version of jjwt is modified in the 
 * following ways:
 * 
 *  1. jjwt customized for tapis is called jjwt-aloe and is available at
 *     https://github.com/richcar58/jjwt-aloe.
 *  2. The main changes are to pom.xml and SignatureAlgorithm.java.
 *  3. The built artifacts (jar files) are available from our local nexus server at
 *     https://maven.tacc.utexas.edu/nexus/content/repositories/thirdparty/io/jsonwebtoken/jjwt-aloe/0.9.1/.
 *
 * This program runs two tests.  The first (useKeyGen) uses jjwt facilities to generate
 * a key pair that is then used to sign and verify a JWT.  The second (useKeystore)
 * uses keys that pre-exist in a local keystore to sign and verify a JWT.  This latter
 * test simulates how Tapis will verify JWTs in develop, staging and production environments.     
 * 
 * NOTE: The pre-existing keys should be generated using the KeyManagerTest program. 
 * 
 * @author rcardone
 */
public class JJWTTest 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    private static final String TEST_KEY_STORE_PASSWORD = "!akxK3CuHfqzI#97";
    private static final String KEY_ALIAS = "jwt";
    
    // Predefined keystore file names.
    private static final String TEST_STORE_FILE_NAME  = ".TapisTestKeyStore.p12";
    private static final String PROD_STORE_FILE_NAME  = ".TapisKeyStore.p12"; 

    /* **************************************************************************** */
    /*                                    Enum                                      */
    /* **************************************************************************** */
    // Input values used to determine the name of the keystore file.
    public enum KeyStoreFile {TEST, PROD}
    
    /* **************************************************************************** */
    /*                                   Fields                                     */
    /* **************************************************************************** */
    // Input fields.
    private String _keystoreFilename;
    private String _alias;
    private String _password;
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* main:                                                                        */
    /* ---------------------------------------------------------------------------- */
    /** Run the test program using zero or more optional parameters.
     * 
     *      JJWTTest <keystore-file> <alias> <password>
     *  
     *  keystore-file - 'test' for the test keystore, 'prod' for the production keystore [test]
     *  alias         - the name of the key pair to be used [jwt]
     *  password      - the keystore and alias password [default password]
     * 
     * @param args zero to three arguments as described above
     * @throws Exception on error
     */
    public static void main(String[] args) throws Exception
    {
        // ------------------------- Input ------------------------------
        // Create the test instance.
        JJWTTest test = new JJWTTest();
        
        // Determine keystore.
        if (args.length > 0) {
            KeyStoreFile storeFile;
            try {storeFile = KeyStoreFile.valueOf(args[0].toUpperCase());}
                catch (Exception e) {
                    System.out.println(getHelpMessage());
                    return;
                }
            
            // Assign instance field.
            if (storeFile == KeyStoreFile.PROD) 
                test._keystoreFilename = PROD_STORE_FILE_NAME;
              else test._keystoreFilename = TEST_STORE_FILE_NAME;
        }
          else test._keystoreFilename = TEST_STORE_FILE_NAME; // default
        
        // Determine alias.
        if (args.length > 1) test._alias = args[1];
          else test._alias = KEY_ALIAS;
        
        // Determine password.
        if (args.length > 2) test._password = args[2];
          else test._password = TEST_KEY_STORE_PASSWORD;
        
        // ------------------------- Processing -------------------------
        // Pring start up information.
        System.out.println(">> Starting JJWTTest >>\n");
        System.out.println("  alias = " + test._alias);
        System.out.println("  keystore = " + test._keystoreFilename);
        
        // Test 1.
        System.out.println("\nSigning and verify a JWT using newly generated keys");
        System.out.println("---------------------------------------------------\n");
        test.useKeyGen();
        
        // Test 2.
        System.out.println("\nSiging and verify a JWT using pre-existing keys from keystore");
        System.out.println("-------------------------------------------------------------\n");
        test.useKeystore();
        
        System.out.println("\n<< Terminating JJWTTest <<");
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* useKeyGen:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private void useKeyGen()
    {
        // Create keys.
        KeyPair keyPair = RsaProvider.generateKeyPair();
        
        // Reusable codec.
        Encoder b64Encoder = Base64.getUrlEncoder();

        // Print keys.
        System.out.println("**** Public Key Information");
        System.out.println("  algorithm: " + keyPair.getPublic().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPublic().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPublic().getEncoded()));
        System.out.println("**** End Public Key Information");
        System.out.println("**** Private Key Information");
        System.out.println("  algorithm: " + keyPair.getPrivate().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPrivate().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPrivate().getEncoded()));
        System.out.println("**** End Private Key Information\n");
        
        // Create and print json.
        Claims claims = Jwts.claims();
        claims.put("fruit", "banana");
        String json = Jwts.builder().setSubject("bud").setClaims(claims).signWith(keyPair.getPrivate(), SignatureAlgorithm.RS384).compact();
        System.out.println("Generated JWT:");
        System.out.println(json);
        
        // Validate JWT.
        Jwt jwt = Jwts.parser().setSigningKey(keyPair.getPublic()).parse(json);
        Header header = jwt.getHeader();
        Object body = jwt.getBody();
        System.out.println("Validating JWT:");
        System.out.println("  header type: " + header.getClass().getName());
        System.out.println("  body type: " + body.getClass().getName());
        
        // Print results.
        System.out.println("Headers");
        Set<Entry<String,Object>> entries = header.entrySet();
        for (Entry<String,Object> entry : entries) 
            System.out.println("  " + entry.getKey() + ": " + (entry.getValue() == null ? "null" : entry.getValue().toString()));
        
        System.out.println("Claims");
        Claims parsedClaims = (Claims) body;
        Set<Entry<String,Object>> claimSet = parsedClaims.entrySet();
        for (Entry<String,Object> entry : claimSet) 
            System.out.println("  " + entry.getKey() + ": " + (entry.getValue() == null ? "null" : entry.getValue().toString()));
    }
    
    /* ---------------------------------------------------------------------------- */
    /* useKeystore:                                                                 */
    /* ---------------------------------------------------------------------------- */
    /** Retrieve a key pair from the user-designated key store and create and verify
     * a JWT using those keys.
     * 
     * @throws Exception on error
     */
    private void useKeystore() throws Exception
    {
        // ----- Load the keystore.
        KeyManager km = new KeyManager(null, _keystoreFilename);
        km.load(_password);
        
        // ----- Get the private key from the keystore.
        System.out.println("Retrieving private key and certificate containing public key.");
        PrivateKey privateKey = km.getPrivateKey(_alias, _password);
        Certificate cert = km.getCertificate(_alias);
        PublicKey publicKey = cert.getPublicKey();
        
        // ----- Verify certificate.
        System.out.println("Verifying the certificate.");
        cert.verify(publicKey);
        KeyPair keyPair = new KeyPair(publicKey, privateKey);
        
        // Reusable codec.
        Encoder b64Encoder = Base64.getUrlEncoder();

        // Print keys.
        System.out.println("**** Public Key Information");
        System.out.println("  algorithm: " + keyPair.getPublic().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPublic().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPublic().getEncoded()));
        System.out.println("**** End Public Key Information");
        System.out.println("**** Private Key Information");
        System.out.println("  algorithm: " + keyPair.getPrivate().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPrivate().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPrivate().getEncoded()));
        System.out.println("**** End Private Key Information\n");
        
        // Create and print json.
        Claims claims = Jwts.claims();
        claims.put("fruit", "apple");
        String json = Jwts.builder().setClaims(claims).setSubject("bud").signWith(keyPair.getPrivate(), SignatureAlgorithm.RS384).compact();
        System.out.println("Generated JWT:");
        System.out.println(json);
        
        // Validate JWT.
        Jwt jwt = Jwts.parser().setSigningKey(keyPair.getPublic()).parse(json);
        Header header = jwt.getHeader();
        Object body = jwt.getBody();
        System.out.println("Validating JWT:");
        System.out.println("  header type: " + header.getClass().getName());
        System.out.println("  body type: " + body.getClass().getName());
        
        // Print results.
        System.out.println("Headers");
        Set<Entry<String,Object>> entries = header.entrySet();
        for (Entry<String,Object> entry : entries) 
            System.out.println("  " + entry.getKey() + ": " + (entry.getValue() == null ? "null" : entry.getValue().toString()));
        
        System.out.println("Claims");
        Claims parsedClaims = (Claims) body;
        Set<Entry<String,Object>> claimSet = parsedClaims.entrySet();
        for (Entry<String,Object> entry : claimSet) 
            System.out.println("  " + entry.getKey() + ": " + (entry.getValue() == null ? "null" : entry.getValue().toString()));
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getHelpMessage:                                                              */
    /* ---------------------------------------------------------------------------- */
    private static String getHelpMessage()
    {
        String msg = JJWTTest.class.getSimpleName() + " tests JWT creation and validation.\n\n";
        msg += "All of the following input parameters are optional:\n\n";
        msg += "JJWTTest <keystore-file> <alias> <password>\n\n";
        msg += "    keystore-file - 'test' for the test keystore, 'prod' for the production keystore [test]\n";
        msg += "    alias         - the name of the key pair to be used [jwt]\n";
        msg += "    password      - the keystore and alias password [default password]\n\n";
        return msg;
    }
}
    
