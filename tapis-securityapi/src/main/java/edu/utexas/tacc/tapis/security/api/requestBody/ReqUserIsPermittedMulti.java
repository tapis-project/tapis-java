package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqUserIsPermittedMulti
 implements IReqBody
{
    public String tenant;
    public String user;
    public String[] permSpecs;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "permSpecs", "user");
        if (permSpecs == null || (permSpecs.length == 0))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "permSpecs", "permSpecs");
        
        // Check each permission specification.
        for (String permSpec : permSpecs)
            if (StringUtils.isBlank(permSpec)) 
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "isPermittedMulti", "permSpec");
        
        // Success.
        return null;
    }
}
