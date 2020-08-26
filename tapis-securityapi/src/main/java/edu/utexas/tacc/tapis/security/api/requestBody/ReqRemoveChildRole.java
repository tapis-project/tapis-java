package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqRemoveChildRole
 extends ReqAddChildRole
{
    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(roleTenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "roleTenant");
        if (StringUtils.isBlank(parentRoleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "parentRoleName");
        if (StringUtils.isBlank(childRoleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "removeChildRole", "childRoleName");
        
        // Success.
        return null;
    }

}
