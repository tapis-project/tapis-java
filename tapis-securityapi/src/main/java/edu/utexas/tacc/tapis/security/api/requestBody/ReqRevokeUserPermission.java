package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqRevokeUserPermission
 implements IReqBody
{
    public String tenant;
    public String user;
    public String permSpec;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "revokeUserPermission", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "revokeUserPermission", "user");
        if (StringUtils.isBlank(permSpec)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "revokeUserPermission", "permSpec");
        
        // Success.
        return null;
    }
}
