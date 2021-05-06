package edu.utexas.tacc.security.api.secrets;

import java.util.HashMap;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.client.gen.model.SkSecret;
import edu.utexas.tacc.tapis.security.client.model.SKSecretMetaParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretReadParms;
import edu.utexas.tacc.tapis.security.client.model.SKSecretWriteParms;
import edu.utexas.tacc.tapis.security.client.model.SecretType;

/** This test program exercises the JWTSigning key generation and retrieval capabilities 
 * of SK.  This program only runs when the tokensJwt environment variable has been set
 * to a valid service JWT for the Tokens service at the target site. 
 * 
 * The baseUrl environment variable also be set to SK's url.  If not set, a default
 * development url is used.
 * 
 * @author rcardone
 */
public class GenerateJWTSigningKeys 
{
    // -------- Constants
    //
    // Set destroy=true and write=false when you want to clean up completely.
    private static final boolean DESTROY_SECRET = false;
    private static final boolean WRITE_SECRET   = true;
    
    private static final String JWT_ENV_VAR = "tokensJwt";
    private static final String BASE_URL = "baseUrl";
    
    private static final String TENANT = "fakeTenant";
    private static final String USER   = "fakeUser";
    
    private static final String DEFAULT_BASE_URL = "https://dev.develop.tapis.io/v3";
    
    // -------- Public Methods
    public static void main(String[] args) throws TapisClientException 
    {
        // Get the required Tokens service JWT from the environment.
        String tokensJwt = System.getenv(JWT_ENV_VAR);
        if (StringUtils.isBlank(tokensJwt)) {
            String msg = "The environment variable " + JWT_ENV_VAR + " must be set to the service JWT of the site's Token service.";
            throw new NotFoundException(msg);
        }
        
        // Get the optional baseUrl from the environment.
        String baseUrl = System.getenv(BASE_URL);
        if (StringUtils.isBlank(baseUrl)) baseUrl = DEFAULT_BASE_URL;
        
        // Run the program.
        var generator = new GenerateJWTSigningKeys();
        generator.generate(tokensJwt, baseUrl);
    }
    
    // -------- Private Methods
    /** Issue these SK calls:
     * 
     *      checkhealth       - sanity check
     *      readSecret        - see if the fake secret already exists
     *      writeSecret       - (optional) write a new version of the secret
     *      readSecret        - get the latest version of the secret
     *      destroySecretMeta - (optional) remove the secret and its metadata
     * 
     * @param tokensJwt
     * @param baseUrl
     * @throws TapisClientException
     */
    private void generate(String tokensJwt, String baseUrl) throws TapisClientException
    {
        // Let's communicate with SK.
        var skClient = new SKClient(baseUrl, tokensJwt);
        skClient.addDefaultHeader("X-Tapis-User", "tokens");
        skClient.addDefaultHeader("X-Tapis-Tenant", "admin");
        skClient.addDefaultHeader("Content-Type", "application/json");
        
        // First check SK health.
        String healthCheck = skClient.checkHealth();
        System.out.println("healthcheck: " + healthCheck);
        
        // Read the secret first to see if it exists.
        SkSecret skSecret = null;
        try {skSecret = readSecret(skClient);}
            catch (Exception e) {} // Exception thrown when not found.
        if (skSecret == null) System.out.println("No signing keys found.");
          else System.out.println(formatSkSecret(skSecret));
        
        // Make the write call.
        if (WRITE_SECRET) writeSecret(skClient);
        
        // Read the new version of the secret.
        try {skSecret = readSecret(skClient);}
            catch (Exception e) {if (WRITE_SECRET) e.printStackTrace();}
        if (skSecret == null) System.out.println("No signing keys found.");
          else System.out.println(formatSkSecret(skSecret));
    
        // Conditionally destroy all remnants of a secret.
        if (DESTROY_SECRET) destroySecretMeta(skClient);
        
        skClient.close();
    }
    
    private void writeSecret(SKClient skClient) throws TapisClientException
    {
        var map = new HashMap<String,String>();
        map.put("privateKey", "<generate-secret>");
        var parms = new SKSecretWriteParms(SecretType.JWTSigning);
        parms.setTenant(TENANT);
        parms.setUser(USER);
        parms.setSecretName("keys");
        parms.setData(map);
        skClient.writeSecret(TENANT, USER, parms);
    }
    
    private SkSecret readSecret(SKClient skClient) throws TapisClientException
    {
        var parms = new SKSecretReadParms(SecretType.JWTSigning);
        parms.setSecretName("keys");
        parms.setTenant(TENANT);
        parms.setUser(USER);
        var skSecret = skClient.readSecret(parms);
        return skSecret;
    }
    
    private void destroySecretMeta(SKClient skClient) throws TapisClientException
    {
        var parms = new SKSecretMetaParms(SecretType.JWTSigning);
        parms.setTenant(TENANT);
        parms.setUser(USER);
        parms.setSecretName("keys");
        skClient.destroySecretMeta(parms);
    }
    
    private String formatSkSecret(SkSecret skSecret)
    {
        var meta = skSecret.getMetadata();
        int    version = meta.getVersion();
        String created = meta.getCreatedTime();
        
        // Get all the keys.
        String s = "=========================== SECRET \n"; 
        s += "Secrets version " + version + " created at " + created + ":\n";
        for (var entry : skSecret.getSecretMap().entrySet())
            s += entry.getKey() + ":\n" + entry.getValue() + "\n";
        
        return s;
    }
}
