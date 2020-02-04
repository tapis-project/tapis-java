package edu.utexas.tacc.tapis.security.secrets;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** This class maps URLs to Vault paths. All secret endpoints have url paths 
 * that begin as follows:
 * 
 * <pre>
 *      /v3/security/vault/secret/<secretTypeString>/
 * </pre>
 *      
 * The secretTypeStrings are the urlText value assigned in each SecretType enum.  
 * The purpose of this class is to transform input url paths to Vault KV v2 secrets 
 * engine paths.  Query parameter values passed in with the request are used to 
 * complete the Vault paths.  Each secretType has a set of query parameters that 
 * are required to be present on the request in order to successfully construct 
 * their Vault paths. These required parameters are checked during path construction.
 * 
 * Here are the templates of the Vault paths constructed for each secretType:
 * <p>dbcred:
 * 
 * <pre>
 *  /tapis/service/<service>/dbhost/<host>/dbname/<dbname>/dbuser/<user>/credentials/<secretName>
 * <pre>
 * 
 * <p>jwtsiging:
 * 
 * <pre>
 *  /tapis/tenant/<tenantId>/jwtkey/<secretName>
 * </pre>
 *  
 * <p>system:
 * 
 * <pre>
 *  /tapis/tenant/<tenantId>/system/<systemId>/user/<owner>/sshkey/<secretName>
 *  /tapis/tenant/<tenantId>/system/<systemId>/dynamicUserId/sshkey/<secretName>
 *  /tapis/tenant/<tenantId>/system/<systemId>/user/<owner>/password/<secretName>
 * </pre> 
 * 
 * <p>user:
 * 
 * <pre>
 *  /tapis/tenant/<tenant>/user/<user>/kv/<secretName>
 * </pre>
 * 
 * @author rcardone
 */
public final class SecretPathMapper 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SecretPathMapper.class);
    
    /* ********************************************************************** */
    /*                                 Enums                                  */
    /* ********************************************************************** */
    // The valid types as expected on input.
    private enum KeyType {sshkey, password}
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // All parameters needed to construct a secret path.
    private final SecretPathMapperParms _parms;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SecretPathMapper(SecretPathMapperParms parms)
     throws TapisImplException
    {
        // This should never happen.
        if (parms == null) {
            String msg =  MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "SecretPathMapper", 
                                          "parms");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
        }
        _parms = parms;
    }
        
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getSecretPath:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Construct the vault v2 secret pathname.
     * 
     * @param tenant the caller's tenant
     * @param user the caller userid
     * @param secretName the user-specified secret
     * @return the full secret path
     */
    public String getSecretPath(String tenant, String user)
     throws TapisImplException
    {
        // Result path.
        String secretPath = null;
        
        // Parm validation and path construction depend on the secret type.
        switch (_parms.secretType) 
        {
            case System:
                secretPath = getSystemPath(tenant, user);
                break;
            case User:
                secretPath = getUserPath(tenant, user);
                break;
            case DBCredential:
                secretPath = getDBCredentialPath(tenant, user);
                break;
            case JWTSigning:
                secretPath = getJWTSigningPath(tenant);
                break;
            default:
                // This should never happen as long as all cases are covered.
                var secretTypes = new ArrayList<String>();
                for (SecretType t : SecretType.values()) secretTypes.add(t.name());
                String msg =  MsgUtils.getMsg("SK_VAULT_INVALID_SECRET_TYPE", "SecretPathMapper", 
                                              _parms.secretType, secretTypes);
                _log.error(msg);
                throw new TapisImplException(msg, Condition.INTERNAL_SERVER_ERROR);
        }
        
        return secretPath;
    } 
    
    /* ---------------------------------------------------------------------- */
    /* getUserPath:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Construct the user secret path. 
     * 
     * <pre>
     *  /tapis/tenant/<tenant>/user/<user>/kv/<secretName>
     * </pre>
     *  
     * @param tenant the request tenant
     * @param user the request user
     * @return the Vault secret engine path
     * @throws TapisImplException missing or invalid query parameters
     */
    private String getUserPath(String tenant, String user) 
     throws TapisImplException 
    {
        // Return the path for this secret type.
        return "secret/tapis/tenant/" + tenant + "/user/" + user + "/kv/" + 
               _parms.secretName;
    }

    /* ---------------------------------------------------------------------- */
    /* getSystemPath:                                                         */
    /* ---------------------------------------------------------------------- */
    /** Construct the system secret path.
     * 
     * <pre>
     *  /tapis/tenant/<tenantId>/system/<systemId>/user/<owner>/sshkey/<secretName>
     *  /tapis/tenant/<tenantId>/system/<systemId>/dynamicUserId/sshkey/<secretName>
     *  /tapis/tenant/<tenantId>/system/<systemId>/user/<owner>/password/<secretName>
     * </pre>
     * 
     * @param tenant the request tenant
     * @param user the request user
     * @return the Vault secret engine path
     * @throws TapisImplException missing or invalid query parameters
     */
    private String getSystemPath(String tenant, String user) 
     throws TapisImplException 
    {
        // Check required inputs.
        if (StringUtils.isBlank(_parms.getSysId())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getSystemPath", "sysId");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        // Optional only when using trusted CA. 
        if (!_parms.isDynamicKey() && StringUtils.isBlank(_parms.getSysOwner())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getSystemPath", "sysOwner");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // The key type must always be valid.
        KeyType keyType;
        try {keyType = KeyType.valueOf(_parms.getKeyType());}
            catch (Exception e) {
                var keyTypes = new ArrayList<String>();
                for (KeyType t : KeyType.values()) keyTypes.add(t.name());
                String msg = MsgUtils.getMsg("SK_VAULT_INVALID_KEYTYPE", 
                                             _parms.getKeyType(), keyTypes);
                _log.error(msg);
                throw new TapisImplException(msg, Condition.BAD_REQUEST);
            }
        
        // The type of key determines it's path.
        if (_parms.isDynamicKey()) {
            // Trusted CA case.
            return "secret/tapis/tenant/" + tenant + "/system/" + _parms.getSysId() +
                   "/dynamicUserId/sshkeys/" + _parms.getSecretName(); 
            
        } else if (keyType == KeyType.sshkey) {
            // Distributed key case.
            return "secret/tapis/tenant/" + tenant + "/system/" + _parms.getSysId() +
                    "/user/" + _parms.getSysOwner() + "/sshkey/" +
                    _parms.getSecretName();
        } else {
            // Password case.
            return "secret/tapis/tenant/" + tenant + "/system/" + _parms.getSysId() +
                    "/user/" + _parms.getSysOwner() + "/password/" +
                    _parms.getSecretName();
        }
    }

    /* ---------------------------------------------------------------------- */
    /* getJWTSigningPath:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Construct the jwt signing secret path.
     * 
     * <pre>
     *  /tapis/tenant/<tenantId>/jwtkey/<secretName>
     * </pre>
     * 
     * @param tenant the request tenant
     * @param user the request user
     * @return the Vault secret engine path
     * @throws TapisImplException missing or invalid query parameters
     */
    private String getJWTSigningPath(String tenant) 
     throws TapisImplException 
    {
        // Return the path for this secret type.
        return "secret/tapis/tenant/" + tenant + "/jwtkey/" + _parms.secretName;
    }

    /* ---------------------------------------------------------------------- */
    /* getDBCredentialPath:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Construct the database credentials secret path.
     * 
     * <pre>
     *  /tapis/service/<service>/dbhost/<host>/dbname/<dbname>/dbuser/<user>/credentials/<secretName>
     * <pre>
     * 
     * @param tenant the request tenant
     * @param user the request user
     * @return the Vault secret engine path
     * @throws TapisImplException missing or invalid query parameters
     */
    private String getDBCredentialPath(String tenant, String user)
     throws TapisImplException 
    {
        // Check required inputs.
        if (StringUtils.isBlank(_parms.getDbHost())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDBCredentialPath", "dbhost");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(_parms.getDbName())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getDBCredentialPath", "dbname");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        if (StringUtils.isBlank(_parms.getService())) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "getJWTSigningPath", "service");
            _log.error(msg);
            throw new TapisImplException(msg, Condition.BAD_REQUEST);
        }
        
        // Return the path for this secret type.
        return "secret/tapis/service/" + _parms.getService() + "/dbhost/" + 
               _parms.getDbHost() + "/dbname/" + _parms.getDbName() + "/dbuser" +
               user + "/credentials/" + _parms.getSecretName();
    }

    /* ********************************************************************** */
    /*                        SecretPathMapperParms Class                     */
    /* ********************************************************************** */
    public static final class SecretPathMapperParms 
    {
        // We require a valid secret type.
        public SecretPathMapperParms(SecretType secretType) 
         throws TapisImplException
        {
            if (secretType == null) {
                String msg =  MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "SecretPathMapper", 
                                              "parms");
                _log.error(msg);
                throw new TapisImplException(msg, Condition.BAD_REQUEST);
            }
            this.secretType = secretType;
        }
        
        // The first two parameters are typically URL 
        // path parameters, the rest are typically
        // query parameters.
        private final SecretType secretType;
        private String           secretName;
        private String           sysId;
        private String           sysOwner;
        private boolean          dynamicKey;
        private String           keyType;
        private String           dbHost;
        private String           dbName;
        private String           service;
        
        // Accessors
        public String getSecretName() {return secretName;}
        public void setSecretName(String secretName) {this.secretName = secretName;}
        public String getSysId() {return sysId;}
        public void setSysId(String sysId) {this.sysId = sysId;}
        public String getSysOwner() {return sysOwner;}
        public void setSysOwner(String sysOwner) {this.sysOwner = sysOwner;}
        public boolean isDynamicKey() {return dynamicKey;}
        public void setDynamicKey(boolean dynamicKey) {this.dynamicKey = dynamicKey;}
        public String getKeyType() {return keyType;}
        public void setKeyType(String keyType) {this.keyType = keyType;}
        public String getDbHost() {return dbHost;}
        public void setDbHost(String dbHost) {this.dbHost = dbHost;}
        public String getDbName() {return dbName;}
        public void setDbName(String dbName) {this.dbName = dbName;}
        public SecretType getSecretType() {return secretType;}
        public String getService() {return service;}
        public void setService(String service) {this.service = service;}
    }
}
