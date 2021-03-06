package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqCreateRole
 implements IReqBody
{
    public String roleTenant;
    public String roleName;
    public String description;
    
    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(roleTenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "roleTenant");
        if (StringUtils.isBlank(roleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "roleName");
        if (StringUtils.isBlank(description))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "createRole", "description");
        if (!SKApiUtils.isValidName(roleName))
            return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "createRole", "roleName", roleName);
        
        // Success.
        return null;
    }
}
