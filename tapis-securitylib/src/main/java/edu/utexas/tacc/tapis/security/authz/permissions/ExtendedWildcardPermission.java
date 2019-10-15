package edu.utexas.tacc.tapis.security.authz.permissions;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ExtendedWildcardPermission 
 extends WildcardPermission
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(ExtendedWildcardPermission.class);
    private static final long serialVersionUID = -673253989964825349L;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */

    /* **************************************************************************** */
    /*                                Constructors                                  */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ExtendedWildcardPermission(String wildcardString) {
        super(wildcardString, DEFAULT_CASE_SENSITIVE);
    }

    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public ExtendedWildcardPermission(String wildcardString, boolean caseSensitive) {
        super(wildcardString, caseSensitive);
    }

                                                               
}
