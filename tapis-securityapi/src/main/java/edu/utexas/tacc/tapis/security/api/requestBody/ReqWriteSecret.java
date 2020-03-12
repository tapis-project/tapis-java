package edu.utexas.tacc.tapis.security.api.requestBody;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class ReqWriteSecret
 implements IReqBody
{
    public String  tenant;
    public String  user;
    public Options options;
    public Map<String,String> data;
    
    public static final class Options
    {
        public int cas;
    }

    /** Return a user-appropriate error message on failed validation
     *  and return null if validation succeeds.
     */ 
    @Override
    public String validate() 
    {
        // Final checks.
        if (StringUtils.isBlank(tenant)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "writeSecret", "tenant");
        if (StringUtils.isBlank(user)) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "writeSecret", "user");
        
        // Success.
        return null;
    }
}
