package edu.utexas.tacc.tapis.security.secrets;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public final class SecretTypeDetector 
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // We split vault paths on slashes.
    private static final Pattern SPLIT_PATTERN = Pattern.compile("/");
    
    // Minimum number of segment lengths for each secret type.
    // All but system always have an exact number.
    private static final int NUM_SYSTEM_PARTS = 8; // 8 or 9
    private static final int NUM_DBCRED_PARTS = 11;
    private static final int NUM_JWTSIG_PARTS = 5;
    private static final int NUM_SVCPWD_PARTS = 7;
    private static final int NUM_USER_PARTS   = 7;
    
    // We only detect paths that begin with this exact prefix.
    private static final String PATH_PREFIX = "tapis/";
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* detectType:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Give a path of a secret in vault, return the type of SK secret it 
     * represents.  If the path does not begin with "tapis/", null is returned.
     * Null is also returned if the path does not match any patterned expected
     * for a tapis secret.
     * 
     * @param path a vault path starting with tapis/
     * @return the type of SK secret the path represents or null if unidentifiable
     */
    public static SecretType detectType(String path)
    {
        // Validate input.
        if (StringUtils.isBlank(path) || !path.startsWith(PATH_PREFIX)) return null;
        
        // Split the path into segments.
        var parts = SPLIT_PATTERN.split(path, 0);
        
        // Check each type one at a time. Since the types are mutually exclusive 
        // only one should ever succeed.  See SecretPathMapper for segment mappings.
        if (parts.length >= NUM_SVCPWD_PARTS) {
            // tapis/tenant/" + tenant + "/service/" + user + "/kv/"
            if ("tenant".equals(parts[1]) && "service".equals(parts[3]) &&
                "kv".equals(parts[5])) return SecretType.ServicePwd;
        }
        
        if (parts.length >= NUM_DBCRED_PARTS) {
            // tapis/service/" + _parms.getDbService() + "/dbhost/" + 
            // _parms.getDbHost() + "/dbname/" + _parms.getDbName() + "/dbuser/" +
            // user + "/credentials/"
            if ("service".equals(parts[1]) && "dbhost".equals(parts[3]) && 
                "dbname".equals(parts[5]) && "dbuser".equals(parts[7])  &&
                "credentials".equals(parts[9])) return SecretType.DBCredential;
        }
        
        if (parts.length >= NUM_JWTSIG_PARTS) {
            // tapis/tenant/" + tenant + "/jwtkey/" + _parms.secretName
            if ("tenant".equals(parts[1]) && "jwtkey".equals(parts[3])) 
                return SecretType.JWTSigning;
        }
        
        if (parts.length >= NUM_SYSTEM_PARTS) {
            // tapis/tenant/<tenantId>/system/<systemId>/user/<user>
            // tapis/tenant/<tenantId>/system/<systemId>/dynamicUserId
            if ("tenant".equals(parts[1]) && "system".equals(parts[3]) &&
                ("user".equals(parts[5]) || "dynamicUserId".equals(parts[5])))
                return SecretType.System;
        }
        
        if (parts.length >= NUM_USER_PARTS) {
            // tapis/tenant/" + tenant + "/user/" + user + "/kv/"
            if ("tenant".equals(parts[1]) && "user".equals(parts[3]) &&
                "kv".equals(parts[5]))
                return SecretType.User;
        }
        
        // All secret paths should be identifiable, so we should never get here.
        return null;
    }
}
