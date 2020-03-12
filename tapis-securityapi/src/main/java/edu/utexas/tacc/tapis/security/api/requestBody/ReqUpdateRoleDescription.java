package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqUpdateRoleDescription 
 implements IReqBody
{
    public String tenant;
    public String user;
    public String description;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "user");
        if (StringUtils.isBlank(description)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "description");
        
        // Success.
        return null;
    }
}

