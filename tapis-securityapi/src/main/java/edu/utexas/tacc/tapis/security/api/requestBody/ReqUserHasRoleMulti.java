package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqUserHasRoleMulti
 implements IReqBody
{
    public String tenant;
    public String user;
    public String[] roleNames;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "user");
        if (roleNames == null || (roleNames.length == 0))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "roleNames");

        // Check each role name.
        for (String roleName : roleNames) {
            if (StringUtils.isBlank(roleName)) 
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRoleMulti", "roleName");
            if (!SKApiUtils.isValidName(roleName))
                return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "hasRoleMulti", "roleName", roleName);
        }
        
        // Success.
        return null;
    }
}
