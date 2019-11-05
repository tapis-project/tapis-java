package edu.utexas.tacc.tapis.sharedapi.jaxrs.filters;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext.AccountType;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import edu.utexas.tacc.tapis.sharedapi.utils.TapisRestUtils;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;

/** This jax-rs filter performs the following:
 * 
 *      - Reads the tapis jwt assertion header from the http request.
 *      - Determines whether the header is required and takes appropriate action.
 *      - Extracts the tenant id from the unverified claims.
 *      - Optionally verifies the JWT signature using a tenant-specific key.
 *      - Extracts the user name and other values from the JWT claims.
 *      - Assigns claim values to their thread-local fields.
 *      
 * The test parameter filter run after this filter and may override the values
 * set by this filter.
 * 
 * @author rcardone
 */
@Provider
@Priority(Priorities.AUTHENTICATION)
public class JWTValidateRequestFilter 
 implements ContainerRequestFilter
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JWTValidateRequestFilter.class);
    
    // Header key for jwts.
    private static final String TAPIS_JWT_HEADER = "X-Tapis-Token";
    
    // Tapis claim keys.
    private static final String CLAIM_TENANT         = "tapis/tenant_id";
    private static final String CLAIM_USERNAME       = "tapis/username";
    private static final String CLAIM_TOKEN_TYPE     = "tapis/token_type";
    private static final String CLAIM_ACCOUNT_TYPE   = "tapis/account_type";
    private static final String CLAIM_DELEGATION     = "tapis/delegation";
    private static final String CLAIM_DELEGATION_SUB = "tapis/delegation_sub";
    
    // The token types this filter expects.
    private static final String TOKEN_ACCESS = "access";
    
    // TODO: Hardcode signature verification key for all tenants until SK becomes available.
    private static final String TEMP_TAPIS_PUBLIC_KEY = 
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAz7rr5CsFM7rHMFs7uKIdcczn0uL4ebRMvH8pihrg1tW/fp5Q+5ktltoBTfIaVDrXGF4DiCuzLsuvTG5fGElKEPPcpNqaCzD8Y1v9r3tfkoPT3Bd5KbF9f6eIwrGERMTs1kv7665pliwehz91nAB9DMqqSyjyKY3tpSIaPKzJKUMsKJjPi9QAS167ylEBlr5PECG4slWLDAtSizoiA3fZ7fpngfNr4H6b2iQwRtPEV/EnSg1N3Oj1x8ktJPwbReKprHGiEDlqdyT6j58l/I+9ihR6ettkMVCq7Ho/bsIrwm5gP0PjJRvaD5Flsze7P4gQT37D1c5nbLR+K6/T0QTiyQIDAQAB";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List all of url substrings that identify authentication exempt requests.
    private static final HashSet<String> _noAuthRequests = initNoAuthRequests();
    
    // The public key used to check the JWT signature.  This cached copy is
    // used by all instances of this class.
    private static PublicKey _jwtPublicKey;
    
    /* ********************************************************************** */
    /*                            Public Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* filter:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void filter(ContainerRequestContext requestContext) 
    {
        // Tracing.
        if (_log.isTraceEnabled())
            _log.trace("Executing JAX-RX request filter: " + this.getClass().getSimpleName() + ".");
        
        // Skip JWT processing for non-authenticated requests.
        if (isNoAuthRequest(requestContext)) return;   
        
        // ------------------------ Extract Encoded JWT ------------------------
        // Parse variables.
        String encodedJWT = null;
        
        // Extract the jwt header from the set of headers.
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        for (Entry<String, List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase(TAPIS_JWT_HEADER)) {
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty())
                    encodedJWT = values.get(0);
                
                // We're done.
                break;
            }
        }
            
        // Make sure that a JWT was provided when it is required.
        if (StringUtils.isBlank(encodedJWT)) {
            // This is an error in production, but allowed when running in test mode.
            // We let the endpoint verify that all needed parameters have been supplied.
            boolean jwtOptional = TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL);
            if (jwtOptional) return;
            
            // We abort the request because we're missing required security information.
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_MISSING_JWT_INFO", requestContext.getMethod());
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // ------------------------ Read Tenant Claim --------------------------
        // Get the JWT without verifying the signature.  Decoding checks that
        // the token has not expired.
        Jwt unverifiedJwt = null;
        try {unverifiedJwt = decodeJwt(encodedJWT);}
        catch (Exception e) {
            // Preserve the decoder method's message.
            String msg = e.getMessage();
            _log.error(msg); // No need to log the stack trace again.
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Get the claims.
        Claims claims = null;
        try {claims = (Claims) unverifiedJwt.getBody();}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_GET_CLAIMS", unverifiedJwt);
            _log.error(msg, e);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        if (claims == null) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_NO_CLAIMS", unverifiedJwt);
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Retrieve the user name from the claims section.
        String tenant = (String)claims.get(CLAIM_TENANT);
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_CLAIM_NOT_FOUND", unverifiedJwt, 
                                         CLAIM_TENANT);
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
            
        // ------------------------ Verify JWT ---------------------------------
        // Do we need to verify the JWT?
        boolean skipJWTVerify = TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_SKIP_JWT_VERIFY);
        if (!skipJWTVerify) {
            try {verifyJwt(encodedJWT, tenant);}
            catch (Exception e) {
                Status status = Status.UNAUTHORIZED;
                String msg = e.getMessage();
                if (msg.startsWith("TAPIS_SECURITY_JWT_KEY_ERROR"))
                    status = Status.INTERNAL_SERVER_ERROR;
                _log.error(e.getMessage(), e);
                requestContext.abortWith(Response.status(status).entity(e.getMessage()).build());
                return;
            }
        }
        
        // ------------------------ Validate Claims ----------------------------
        // Check that the token is always an access token.
        String tokenType = (String)claims.get(CLAIM_TOKEN_TYPE);
        if (StringUtils.isBlank(tokenType) || !TOKEN_ACCESS.contentEquals(tokenType)) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", CLAIM_TOKEN_TYPE,
                                         tokenType);
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Check the account type.
        String accountTypeStr = (String)claims.get(CLAIM_ACCOUNT_TYPE);
        if (StringUtils.isBlank(accountTypeStr)) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", CLAIM_ACCOUNT_TYPE,
                                         accountTypeStr);
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        AccountType accountType = null;
        try {accountType = AccountType.valueOf(accountTypeStr);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", CLAIM_ACCOUNT_TYPE,
                                         accountTypeStr);
            _log.error(msg, e);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Get the user.
        String user = (String)claims.get(CLAIM_USERNAME);
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", CLAIM_USERNAME, user);
            _log.error(msg);
            requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Get the delation information if it exists.
        String delegator = null;
        Boolean delegation = (Boolean)claims.get(CLAIM_DELEGATION);
        if (delegation != null && delegation) {
            delegator = (String)claims.get(CLAIM_DELEGATION_SUB);
            if (!TapisRestUtils.checkJWTSubjectFormat(delegator)) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_INVALID_CLAIM", CLAIM_DELEGATION_SUB,
                                             delegator);
                _log.error(msg);
                requestContext.abortWith(Response.status(Status.UNAUTHORIZED).entity(msg).build());
                return;
            }
        }
        
        // ------------------------ Assign Claim Values ------------------------
        // Assign pertinent claims to our threadlocal context.
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        threadContext.setTenantId(tenant);
        threadContext.setUser(user);
        threadContext.setAccountType(accountType);
        threadContext.setDelegatorSubject(delegator);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* decodeJwt:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Decode the jwt without verifying its signature.
     * 
     * @param encodedJWT the JWT from the request header
     * @return the decoded but not verified jwt
     * @throws TapisSecurityException on error
     */
    private Jwt decodeJwt(String encodedJWT)
     throws TapisSecurityException
    {
        // Some defensive programming.
        if (encodedJWT == null) return null;
        
        // Lop off the signature part of the encoding so that the 
        // jjwt library can parse it without attempting validation.
        // We expect the jwt to contain exactly two periods in 
        // the following encoded format: header.body.signature
        // We need to remove the signature but leave both periods.
        String remnant = encodedJWT;
        int lastDot = encodedJWT.lastIndexOf(".");
        if (lastDot + 1 < encodedJWT.length()) // should always be true
            remnant = encodedJWT.substring(0, lastDot + 1);
        
        // Parse the header and claims. If for some reason the remnant
        // isn't of the form header.body. then parsing will fail.
        Jwt jwt = null;
        try {jwt = Jwts.parser().parse(remnant);}
            catch (Exception e) {
                // The decode may have detected an expired JWT.
                String msg;
                String emsg = e.getMessage();
                if (emsg != null && emsg.startsWith("JWT expired at")) 
                    msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_EXPIRED", emsg);
                  else msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_PARSE_ERROR", emsg);
                
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }
        return jwt;
    }
    
    /* ---------------------------------------------------------------------- */
    /* verifyJwt:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Verify the jwt as it was received as a header value.  Signature verification
     * occurs using the specified tenant's signing key.  An exception is thrown
     * if decoding or signature verification fails.
     * 
     * @param encodedJwt the raw jwt
     * @param tenant the tenant to verify against
     * @throws TapisSecurityException if the jwt cannot be verified 
     */
    private void verifyJwt(String encodedJwt, String tenant) 
     throws TapisSecurityException
    {
        // Get the public part of the signing key.
        PublicKey publicKey = getJwtPublicKey(tenant);
        //PublicKey publicKey = getJwtPublicKeyFromTestKeyStore();
        
        // Verify and import the jwt data.
        Jwt jwt = null; 
        try {jwt = Jwts.parser().setSigningKey(publicKey).parse(encodedJwt);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_PARSE_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJwtPublicKey:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Return the cached public key if it exists.  If it doesn't exist, load it
     * from the keystore, cache it, and then return it. 
     * 
     * This exceptions thrown by this method all use the TAPIS_SECURITY_JWT_KEY_ERROR
     * message.  This message is used by calling routines to distinguish between
     * server and requestor errors.
     * 
     * @param tenant the tenant whose signature verification key is requested
     * @return the tenant's signature verification key
     * @throws TapisSecurityException on error
     */
    private PublicKey getJwtPublicKey(String tenant)
     throws TapisSecurityException
     {
        // Use the cached copy if it has already been loaded.
        if (_jwtPublicKey != null) return _jwtPublicKey;
        
        // Serialize access to this code.
        synchronized (JWTValidateRequestFilter.class) 
        {
            // Maybe another thread loaded the key in the intervening time.
            if (_jwtPublicKey != null) return _jwtPublicKey; 
            
            // TODO: replace with SK call in real code.
            // Decode the base 64 string.
            byte[] publicBytes;
            try {publicBytes = Base64.getDecoder().decode(TEMP_TAPIS_PUBLIC_KEY);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_KEY_ERROR", e.getMessage());
                    _log.error(msg, e);
                    throw new TapisSecurityException(msg, e);
                }
            
            // Create the public key object from the byte array.
            PublicKey publicKey;
            try {
                X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicBytes);
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                publicKey = keyFactory.generatePublic(keySpec);
            }
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_KEY_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }

            // Success!
            _jwtPublicKey = publicKey;
        }
        
        return _jwtPublicKey;
     }
    
    /* ---------------------------------------------------------------------- */
    /* getJwtPublicKeyFromTestKeyStore:                                       */
    /* ---------------------------------------------------------------------- */
    /** TEMPORARY TEST CODE
     * 
     * TODO: remove this code when we switch to using the tokens-api keys
     * 
     * @return
     * @throws TapisSecurityException
     */
    private PublicKey getJwtPublicKeyFromTestKeyStore() 
      throws TapisSecurityException
    {
        // Hardcode parameters for testing...
        String alias = "jwt";
        String password = "!akxK3CuHfqzI#97";
        String keystoreFilename = ".TapisTestKeyStore.p12";
        
        PublicKey publicKey = null;
        try {
            // ----- Load the keystore.
            KeyManager km = new KeyManager(null, keystoreFilename);
            km.load(password);

            // ----- Get the private key from the keystore.
            PrivateKey privateKey = km.getPrivateKey(alias, password);
            Certificate cert = km.getCertificate(alias);
            publicKey = cert.getPublicKey();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_KEY_ERROR", e.getMessage());
            _log.error(msg, e);
            throw new TapisSecurityException(msg, e);
        }
        
        return publicKey;
    }
    
    /* ---------------------------------------------------------------------- */
    /* isNoAuthRequest:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Return true if the requested uri is exempt from authentication.  These
     * request do not contain a JWT so no authentication is possible.
     * 
     * @param requestContext the request context
     * @return true is no authentication is required, false otherwise
     */
    private boolean isNoAuthRequest(ContainerRequestContext requestContext)
    {
        // Get the service-specific path, which is the path after the host:port 
        // segment and includes a leading slash.  
        String relativePath = requestContext.getUriInfo().getRequestUri().getPath();
        
        // No authentication requires an exact match.
        if (_noAuthRequests.contains(relativePath)) return true;
        
        // Authentication required.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initNoAuthRequests:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Populate the set of requests that don't require JWTs.  The requests are
     * identified by strings that start with the slash (inclusive) following the 
     * host:port in the request URL.  Each string in the set identifies a path 
     * to the request's base URI.  This approach allows requests from different 
     * services to safely coexist in the same set since each string contains a
     * service name.     
     * 
     * @return the initialized hash set
     */
    private static HashSet<String> initNoAuthRequests()
    {
        // Create the set of requests that do not require authentication.
        var set = new HashSet<String>();
        set.add("/security/v3/healthcheck");
        set.add("/security/v3/openapi.json");
        set.add("/security/v3/openapi.yaml");
        
        return set;
    }
}
