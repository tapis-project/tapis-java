package edu.utexas.tacc.tapis.security.config;

public interface SkConstants 
{
    // We reserve this character for SK generated names.  In particular, SK generated
    // role names always begin this character and users cannot create such names.
    //
    // To make our life easier, tapis reserved characters that might appear in a
    // URL should be URL safe.  These include alphanumerics [0-9a-zA-Z], 
    // special characters $-_.+!*'(), and URL reserved characters ; / ? : @ = &.
    // In particular, these characters are not URL-safe and need to be escaped: 
    // " < > # % { } | \ ^ ~ [ ] ` and space.
    public static final char RESERVED_NAME_CHAR = '$';
    
    // SK generated user default role names.
    public static final String USER_DEFAULT_ROLE_PREFIX = RESERVED_NAME_CHAR + "$";
    
    // SK generated administrative role name prefix.
    public static final String INTERNAL_ROLE_PREFIX = RESERVED_NAME_CHAR + "!";
    public static final String ADMIN_ROLE_NAME = INTERNAL_ROLE_PREFIX + "tenant_admin";

    // SK user.
    public static final String INTERNAL_USER_PREFIX = RESERVED_NAME_CHAR + "~";
    public static final String SK_USER = INTERNAL_USER_PREFIX + "SecurityKernel";

    // Role name max characters allowed in database.
    public static final int MAX_USER_NAME_LEN = 58;
}
