package edu.utexas.tacc.tapis.sharedapi.jaxrs.filters;

import java.security.KeyStoreException;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisSecurityException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv;
import edu.utexas.tacc.tapis.shared.parameters.TapisEnv.EnvVar;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadContext;
import edu.utexas.tacc.tapis.shared.threadlocal.TapisThreadLocal;
import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;

/** This jax-rs filter performs the following:
 * 
 *      - Reads the jwt assertion header from the http request.
 *      - Determines whether the header is required and takes appropriate action.
 *      - Extracts the tenant id from the key name (!!).
 *      - Decodes and optionally verifies the JWT signature.
 *      - Extracts the user name from the JWT claims.
 *      - Sets the thread-local values for tenantId and user.
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
    
    // Header key prefix for jwts.
    private static final String JWT_PREFIX = "x-jwt-assertion-";
    
    // The JWT key alias.  This is the key pair used to sign JWTs
    // and verify them.
    public static final String DEFAULT_KEY_ALIAS = "wso2";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List all of url substrings that identify authentication exempt requests.
    private static final String[] _noAuthRequests = {};
    
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
        
        // Parse variables.
        String headerTenantId = null;
        String encodedJWT     = null;
        
        // Extract the jwt header from the set of headers.
        MultivaluedMap<String, String> headers = requestContext.getHeaders();
        for (Entry<String, List<String>> entry : headers.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(JWT_PREFIX)) {
                
                // Get the tenant information encoded in the key
                // and transform it to match tenant naming conventions.
                headerTenantId = key.substring(JWT_PREFIX.length());
                headerTenantId = TapisUtils.transformRawTenantId(headerTenantId);
                
                // Get the encoded jwt.
                List<String> values = entry.getValue();
                if ((values != null) && !values.isEmpty())
                    encodedJWT = values.get(0);
                
                // We're done.
                break;
            }
        }
            
        // Make sure that a JWT was provided when it is required.
        if (StringUtils.isBlank(encodedJWT) || StringUtils.isBlank(headerTenantId)) {
            // This is an error in production, but allowed when running in test mode.
            // We let the endpoint verify that all needed parameters have been supplied.
            boolean jwtOptional = TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_JWT_OPTIONAL);
            if (jwtOptional) return;
            
            // We abort the request because we're missing required security information.
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_MISSING_JWT_INFO", requestContext.getMethod());
            _log.error(msg);
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Decode the JWT and optionally validate the JWT signature.
        Jwt jwt = null;
        boolean skipJWTVerify = TapisEnv.getBoolean(EnvVar.TAPIS_ENVONLY_SKIP_JWT_VERIFY);
        try {
            if (skipJWTVerify) jwt = decodeJwt(encodedJWT);
              else jwt = decodeAndVerifyJwt(encodedJWT);
        } 
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_DECODE_ERROR", encodedJWT, e.getMessage());
            _log.error(msg, e);
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Make sure we got a JWT. This shouldn't happen, but in case it does...
        if (jwt == null) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_DECODE_ERROR", encodedJWT,
                                         "Null JWT encountered in " + getClass().getSimpleName() + ".");
            _log.error(msg);
            requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).entity(msg).build());
            return;
        }
        
        // Retrieve the user name from the claims section.
        String user  = null;
        String roles = null;
        Claims claims = (Claims) jwt.getBody();
        if (claims != null) {
            user  = getUser(claims);
            roles = getRoles(claims);
        }
        
        // Assign JWT information to thread-local variables.
        TapisThreadContext threadContext = TapisThreadLocal.tapisThreadContext.get();
        if (!StringUtils.isBlank(headerTenantId)) threadContext.setTenantId(headerTenantId);
        if (!StringUtils.isBlank(user)) threadContext.setUser(user);
        if (!StringUtils.isBlank(roles)) threadContext.setRoles(roles);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* decodeJwt:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Decode the jwt without verifying it signature.
     * 
     * @param encodedJWT the JWT from the request header
     * @return the decoded but not verified jwt
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
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_PARSE_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }
        return jwt;
    }
    
    /* ---------------------------------------------------------------------- */
    /* decodeAndVerifyJwt:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Decode the jwt and use the JWT signature to validate that the header 
     * and payload have not changed. 
     * 
     * @param encodedJWT the JWT from the request header
     * @return the decoded and verified jwt
     */
    private Jwt decodeAndVerifyJwt(String encodedJWT)
     throws TapisSecurityException
    {
        // Some defensive programming.
        if (encodedJWT == null) return null;
        
        // Get the public part of the signing key.
        PublicKey publicKey = getJwtPublicKey();
        
        // Verify and import the jwt data.
        Jwt jwt = null; 
        try {jwt = Jwts.parser().setSigningKey(publicKey).parse(encodedJWT);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_PARSE_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }
        
        // We have a validated jwt.
        return jwt;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getJwtPublicKey:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Return the cached public key if it exists.  If it doesn't exist, load it
     * from the keystore, cache it, and then return it. 
     * 
     * @return 
     */
    private PublicKey getJwtPublicKey()
     throws TapisSecurityException
     {
        // Use the cached copy if it has already been loaded.
        if (_jwtPublicKey != null) return _jwtPublicKey;
        
        // Serialize access to this code.
        synchronized (JWTValidateRequestFilter.class) 
        {
            // Maybe another thread loaded the key in the intervening time.
            if (_jwtPublicKey != null) return _jwtPublicKey; 
            
            // We need to load the key from the keystore.
            // Get our own instance of the key manager 
            // to avoid possible multithreading issues.
            KeyManager km;
            try {km = new KeyManager();}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_NO_KEYSTORE", e.getMessage());
                    _log.error(msg, e);
                    throw new TapisSecurityException(msg, e);
                }
        
            // Get the keystore's password.
            String password = TapisEnv.get(EnvVar.TAPIS_ENVONLY_KEYSTORE_PASSWORD);
            if (StringUtils.isBlank(password)) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_NO_KEYSTORE_PASSWORD");
                _log.error(msg);
                throw new TapisSecurityException(msg);
            }
        
            // Load the complete store.
            try {km.load(password);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_KEYSTORE_LOAD_ERROR", e.getMessage());
                _log.error(msg, e);
                throw new TapisSecurityException(msg, e);
            }
            
            // Get the certificate containing the public key.
            Certificate cert = null;
            try {cert = km.getCertificate(DEFAULT_KEY_ALIAS);}
              catch (KeyStoreException e) {
                  String msg = MsgUtils.getMsg("TAPIS_SECURITY_GET_CERTIFICATE", 
                                               DEFAULT_KEY_ALIAS, e.getMessage());
                  _log.error(msg, e);
                  throw new TapisSecurityException(msg, e);
            }
            
            // Make sure we got a certificate.
            if (cert == null) {
                String msg = MsgUtils.getMsg("TAPIS_SECURITY_CERTIFICATE_NOT_FOUND", 
                                             DEFAULT_KEY_ALIAS, km.getStorePath());
                _log.error(msg);
                throw new TapisSecurityException(msg);
            }
            
            // Get the public key from the certificate and verify the certificate.
            PublicKey publicKey = cert.getPublicKey();
            try {cert.verify(publicKey);} 
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_CERTIFICATE_VERIFY", 
                                                 DEFAULT_KEY_ALIAS, e.getMessage());
                    _log.error(msg, e);
                    throw new TapisSecurityException(msg, e);
                } 
            
            // Success!
            _jwtPublicKey = publicKey;
        }
        
        return _jwtPublicKey;
     }
    
    /* ---------------------------------------------------------------------- */
    /* getUser:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Get the user name or return null.
     * 
     * @param claims the JWT claims object
     * @return the simple user name or null
     */
    private String getUser(Claims claims)
    {
        // The enduser name may have extraneous information around it.
        String s = (String)claims.get("http://wso2.org/claims/enduser");
        if (StringUtils.isBlank(s)) return null;
        else if (s.contains("@")) return StringUtils.substringBefore(s, "@");
        else if (s.contains("/")) return StringUtils.substringAfter(s, "/");
        else return s;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getRoles:                                                              */
    /* ---------------------------------------------------------------------- */
    /** Get the set of roles or return null.
     * 
     * @param claims the JWT claims object
     * @return the user's roles or null
     */
    private String getRoles(Claims claims)
    {
        String s =  (String)claims.get("http://wso2.org/claims/role");
        if (StringUtils.isBlank(s)) return null;
          else return s;
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
        // See if the request's relative path begins with a no-auth prefix.
        for (String noAuthRequest : _noAuthRequests)
        {
            // Skip JWT processing for non-authenticated requests. Requests that 
            // don't require an authentication token don't have a JWT header and 
            // should not rely on threadlocal values that originate from JWT 
            // information.  We can tell from the url relative path whether this
            // request is exempt from authentication.
            String relativePath = requestContext.getUriInfo().getPath();
            if (relativePath != null && relativePath.startsWith(noAuthRequest)) {
                if (_log.isInfoEnabled()) {
                    String msg = MsgUtils.getMsg("TAPIS_SECURITY_NO_AUTH_REQUEST", 
                                                 requestContext.getUriInfo().getAbsolutePath());
                    _log.info(msg);
                }
            
                // No authentication.
                return true;
            }
        }
        
        // Authentication required.
        return false;
    }
}
