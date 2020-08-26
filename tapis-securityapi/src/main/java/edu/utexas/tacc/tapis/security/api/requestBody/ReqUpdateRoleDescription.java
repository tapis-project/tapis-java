package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqUpdateRoleDescription 
 implements IReqBody
{
    public String roleTenant;
    public String newDescription;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(roleTenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "roleTenant");
        if (StringUtils.isBlank(newDescription)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "updateRoleDescription", "newDescription");
        
        // Success.
        return null;
    }
}

