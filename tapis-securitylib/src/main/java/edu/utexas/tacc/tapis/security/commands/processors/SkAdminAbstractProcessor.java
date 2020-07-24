package edu.utexas.tacc.tapis.security.commands.processors;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.security.commands.SkAdminParameters;
import edu.utexas.tacc.tapis.security.commands.model.ISkAdminDeployRecorder;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminAbstractSecret;
import edu.utexas.tacc.tapis.security.commands.model.SkAdminResults;
import edu.utexas.tacc.tapis.security.secrets.SecretType;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;

public abstract class SkAdminAbstractProcessor<T extends SkAdminAbstractSecret> 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SkAdminAbstractProcessor.class);
    
    // Default secret key names.  These are the names of the keys inside the
    // secret map used when writing secrets.  Applications will expect these
    // names upon retrieval.
    public static final String DEFAULT_KEY_NAME         = "password";
    public static final String DEFAULT_PRIVATE_KEY_NAME = "privateKey";
    public static final String DEFAULT_PUBLIC_KEY_NAME  = "publicKey";
    
    // Map of url secret type text to secret type enum.
    private static final HashMap<String,SecretType> _secretTypeMap = initSecretTypeMap();
    
    // We're only interested in the latest version of any secret.
    protected static final Integer DEFAULT_SECRET_VERSION = 0;
   
    /* ********************************************************************** */
    /*                                 Enums                                  */
    /* ********************************************************************** */
    // The operations that can be performed.
    public enum Op {create, update, deploy}

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Secrets to be processed by subclasses.
    protected final List<T>           _secrets;
    protected final SkAdminParameters _parms;
    
    // Share a single instance of SK client shared by
    // all subclass instances.
    protected static SKClient         _skClient;
    
    // Create the singleton instance for all processors to use.
    protected static final SkAdminResults _results = SkAdminResults.getInstance();
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Create base class 
     * 
     * @param secrets
     * @param parms
     * @throws TapisRuntimeException
     */
    protected SkAdminAbstractProcessor(List<T> secrets, SkAdminParameters parms)
    {
        // Assign inputs.
        _secrets = secrets;
        _parms = parms;
        
        // Create a single static SK client only if we are going through SK.
        if (_parms.useSK()) initSkClient(parms);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* create:                                                                */
    /* ---------------------------------------------------------------------- */
    public void create()
    {
        // Is there work?
        if (_secrets == null || _secrets.isEmpty()) return;
        for (T secret : _secrets) if (!secret.failed) create(secret);
    }
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    public void update()
    {
        // Is there work?
        if (_secrets == null || _secrets.isEmpty()) return;
        for (var secret : _secrets) if (!secret.failed) update(secret, Op.update);
    }
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    public void deploy(ISkAdminDeployRecorder recorder)
    {
        // Is there work?
        if (_secrets == null || _secrets.isEmpty()) return;
        for (var secret : _secrets) if (!secret.failed) deploy(secret, recorder);
    }    
    
    /* ---------------------------------------------------------------------- */
    /* close:                                                                 */
    /* ---------------------------------------------------------------------- */
    public void close()
    {
        if (_skClient != null) _skClient.close();
    }
    
    /* ********************************************************************** */
    /*                           Abstract Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* create:                                                                */
    /* ---------------------------------------------------------------------- */
    protected abstract void create(T secret);
    
    /* ---------------------------------------------------------------------- */
    /* update:                                                                */
    /* ---------------------------------------------------------------------- */
    protected abstract void update(T secret, Op op);
    
    /* ---------------------------------------------------------------------- */
    /* deploy:                                                                */
    /* ---------------------------------------------------------------------- */
    protected abstract void deploy(T secret, ISkAdminDeployRecorder recorder);
    
    /* ---------------------------------------------------------------------- */
    /* makeFailureMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    protected abstract String makeFailureMessage(Op op, T secret, String errorMsg);
    
    /* ---------------------------------------------------------------------- */
    /* makeSkippedMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    protected abstract String makeSkippedMessage(Op op, T secret);
    
    /* ---------------------------------------------------------------------- */
    /* makeSuccessMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    protected abstract String makeSuccessMessage(Op op, T secret);
    
    /* ---------------------------------------------------------------------- */
    /* makeSkippedDeployMessage:                                              */
    /* ---------------------------------------------------------------------- */
    protected abstract String makeSkippedDeployMessage(T secret);
    
    /* ********************************************************************** */
    /*                           Private Methods                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* initSkClient:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Create the SK client that will be used on all SK calls.
     * 
     * @return the client
     * @throws TapisRuntimeException
     */
    private static synchronized void initSkClient(SkAdminParameters parms)
    {
        // Has the client already been created.
        if (_skClient != null) return;
        
        // Create the client with the base url of the SK server and a JWT.
        _skClient = new SKClient(parms.baseUrl, parms.jwt);
        
        // Always setting the content type cannot hurt.
        _skClient.addDefaultHeader("Content-Type", "application/json");
        
        // Conditionally set the OBO headers.
        setOboHeaders(_skClient, parms);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setOboHeaders:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Add on-behalf-of headers when a service JWT is being used.
     * 
     * @param skClient the client to be initialized
     * @throws TapisRuntimeException, IllegalArgumentException
     */
    private static void setOboHeaders(SKClient skClient, SkAdminParameters parms)
    {
        // Validate that jwt correctly encodes the header, claims and signature.
        int dotCount = StringUtils.countMatches(parms.jwt, '.');
        if (dotCount != 2) {
            String msg = MsgUtils.getMsg("TAPIS_SECURITY_JWT_PARSE_ERROR", 
                               "Invalid number of periods in JWT: " + dotCount);
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
        
        // Remove the header and signature sections leaving only the claims.
        int lastDot = parms.jwt.lastIndexOf(".");
        String remnant = parms.jwt.substring(0, lastDot);
        int firstDot = remnant.indexOf(".");
        if (firstDot > 0) remnant = remnant.substring(firstDot + 1);
        
        // Decode the claims.  An IllegalArgumentException can be thrown here. 
        String decodedJwt = new String(Base64.getUrlDecoder().decode(remnant));
        JsonObject claims = TapisGsonUtils.getGson().fromJson(decodedJwt, JsonObject.class);
        
        // Determine if this is a user or service jwt.
        String type = claims.get("tapis/account_type").getAsString();
        if (!"service".equals(type)) return;
        
        // Only service account require the additional X-Tapis headers.
        String tenant = claims.get("tapis/tenant_id").getAsString();
        String user   = claims.get("tapis/username").getAsString();
        
        // More checks.
        if (StringUtils.isBlank(tenant)) {
            String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tapis/tenant_id");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
        if (StringUtils.isBlank(user)) {
            String msg = MsgUtils.getMsg("SK_MISSING_PARAMETER", "tapis/username");
            _log.error(msg);
            throw new TapisRuntimeException(msg);
        }
        
        // Set the headers.
        skClient.addDefaultHeader("X-Tapis-Tenant", tenant);
        skClient.addDefaultHeader("X-Tapis-User", user);
    }

    /* ---------------------------------------------------------------------------- */
    /* initSecretTypeMap:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Initialize a map with key secret type url text and value SecretType enumeration. 
     * 
     * @return the map of text to enum
     */
    private static HashMap<String,SecretType> initSecretTypeMap()
    {
        // Get a map of secret type text to secret type enum. The secret
        // type text is what should appear in url paths.
        SecretType[] types = SecretType.values();
        var map = new HashMap<String,SecretType>(1 + types.length * 2);
        for (int i = 0; i < types.length; i++) map.put(types[i].getUrlText(), types[i]);
        return map;
    }
}
