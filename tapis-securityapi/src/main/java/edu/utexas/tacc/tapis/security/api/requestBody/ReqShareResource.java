package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.security.api.utils.SKApiUtils;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqShareResource
 implements IReqBody
{
    public String grantor;
    public String grantee;
    public String tenant;
    public String resourceType;
    public String resourceId1;
    public String resourceId2;  // can be null
    public String privilege;
    
    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(grantor)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "grantor");
        if (StringUtils.isBlank(grantee)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "grantee");
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "tenant");
        if (StringUtils.isBlank(resourceType)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "resourceType");
        if (StringUtils.isBlank(resourceId1))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "resourceId1");
        if (!SKApiUtils.isValidName(privilege))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "shareResource", "privilege");
        
        // Success.
        return null;
    }
}
