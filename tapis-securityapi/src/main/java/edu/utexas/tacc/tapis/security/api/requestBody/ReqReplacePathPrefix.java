package edu.utexas.tacc.tapis.security.api.requestBody;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqReplacePathPrefix 
 implements IReqBody
{
    public String tenant;
    public String user;
    public String schema;
    public String roleName;
    public String oldSystemId;
    public String newSystemId;
    public String oldPrefix;
    public String newPrefix;

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "user");
        if (StringUtils.isBlank(schema)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "schema");
        if (StringUtils.isBlank(oldSystemId))
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "oldSystemId");
        if (StringUtils.isBlank(newSystemId)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "replacePathPrefix", "newSystemId");
        
        // Success.
        return null;
    }
}
