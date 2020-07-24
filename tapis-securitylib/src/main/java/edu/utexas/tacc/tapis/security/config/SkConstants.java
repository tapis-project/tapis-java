package edu.utexas.tacc.tapis.security.config;

public interface SkConstants 
{
    // We reserve this character for SK generated names.  In particular, SK generated
    // role names always begin this character and users cannot create such names.
    //
    // To make our life easier, tapis reserved characters that might appear in a
    // URL should be URL safe.  This is much easier said than done.  Theoretical
    // and de facto differences complicate matter, as does the difference between
    // URIs (RFC 3986) and URLs (RFC 1738), and the fact that certain characters
    // are reserved in some parts of a URL and not in others.  As opposed to the
    // RFCs, readable references include:
    // 
    //      https://en.wikipedia.org/wiki/Percent-encoding
    //      https://perishablepress.com/stop-using-unsafe-characters-in-urls/
    //      https://help.marklogic.com/Knowledgebase/Article/View/251/0/using-url-encoding-to-handle-special-characters-in-a-document-uri
    //
    // URI unreserved characters include alphanumerics [0-9a-zA-Z] and 
    // special characters:   - _ . ~
    //
    // URI reserved characters that probably should be avoided:
    //
    //      ! * ' ( ) ; : @ & = + $ , / ? # [ ]
    //
    // Characters that are not URL-safe and need to be escaped according to 
    // the second web site referenced above:
    //  
    //      " < > # % { } | \ ^ ~ [ ] ` and space
    //
    public static final char RESERVED_NAME_CHAR = '$';
    
    // SK generated user default role names.
    public static final String USER_DEFAULT_ROLE_PREFIX = RESERVED_NAME_CHAR + "$";
    
    // SK generated administrative role name prefix.
    public static final String INTERNAL_ROLE_PREFIX = RESERVED_NAME_CHAR + "!";
    public static final String ADMIN_ROLE_NAME = INTERNAL_ROLE_PREFIX + "tenant_admin";

    // SK user.
    public static final String INTERNAL_USER_PREFIX = RESERVED_NAME_CHAR + "~";
    public static final String SK_USER = INTERNAL_USER_PREFIX + "SecurityKernel";

    // Other Tapis internally used roles.
    public static final String TENANT_TOKEN_GENERATOR_ROLE_SUFFIX  = "_token_generator";
    public static final String TENANT_CREATOR_ROLE = "tenant_creator";

    // Role name max characters allowed in database.
    public static final int MAX_USER_NAME_LEN = 58;
}
