package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqGrantUserRoleWithPermission 
 implements IReqBody
{
    public String tenant;
    public String user;
    public String roleName;
    public String permSpec;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantUserRoleWithPerm", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantUserRoleWithPerm", "user");
        if (StringUtils.isBlank(roleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantUserRoleWithPerm", "roleName");
        if (!SKApiUtils.isValidName(roleName))
            return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "grantUserRoleWithPerm", "roleName", roleName);
        if (StringUtils.isBlank(permSpec)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "grantUserRoleWithPerm", "permSpec");
        
        // Success.
        return null;
    }
}
