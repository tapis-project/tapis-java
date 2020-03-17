package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqUserHasRole
 implements IReqBody
{
    public String tenant;
    public String user;
    public String roleName;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "user");
        if (StringUtils.isBlank(roleName)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "hasRole", "roleName");
        if (!SKApiUtils.isValidName(roleName))
            return MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "hasRole", "roleName", roleName);
        
        // Success.
        return null;
    }
}
