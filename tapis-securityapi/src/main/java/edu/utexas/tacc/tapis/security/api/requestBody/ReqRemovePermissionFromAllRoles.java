package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqRemovePermissionFromAllRoles 
 implements IReqBody
{
    public String  tenant;
    public String  permSpec;
    
    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "ReqRemovePermissionFromAllRoles", "tenant");
        if (StringUtils.isBlank(permSpec))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "ReqRemovePermissionFromAllRoles", "permSpec");
        
        // Success.
        return null;
    }
}
