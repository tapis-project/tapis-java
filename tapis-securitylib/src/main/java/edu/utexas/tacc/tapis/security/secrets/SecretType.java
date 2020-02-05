package edu.utexas.tacc.tapis.security.secrets;

/** The types of Tapis secrets that can be manipulated via REST calls.
 * Each type implies a transformation of the URL path to a path in a 
 * Vault secrets engine.  In many cases, required query parameters on
 * the REST call are needed to construct the Vault path.  
 * 
 * @author rcardone
 */
public enum SecretType 
{
    System("system"),
    DBCredential("dbcred"),
    JWTSigning("jwtsigning"),
    User("user"),
    ServicePwd("service");
    
    // The exact text that appears in a request's url path.
    private final String _urlText;
    
    // Constructor.
    private SecretType(String urlText){_urlText = urlText;}
    
    // Accessors.
    public String getUrlText() {return _urlText;}
}
