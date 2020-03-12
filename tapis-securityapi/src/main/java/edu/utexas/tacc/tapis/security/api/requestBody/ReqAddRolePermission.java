package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class ReqAddRolePermission 
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
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "user");
        if (StringUtils.isBlank(roleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "roleName");
        if (!SKApiUtils.isValidName(roleName))
            return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "addRolePermission", "roleName", roleName);
        if (StringUtils.isBlank(permSpec))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "addRolePermission", "permSpec");
        
        // Success.
        return null;
    }
}
