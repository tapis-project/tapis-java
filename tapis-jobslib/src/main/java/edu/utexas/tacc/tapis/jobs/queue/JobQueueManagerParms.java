package edu.utexas.tacc.tapis.jobs.queue;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedq.QueueManagerParms;

public class JobQueueManagerParms
 extends QueueManagerParms
{
    private String  adminUser;
    private String  adminPassword;
    
    public void validate() throws TapisRuntimeException
    {
        // Validate superclass fields.
        super.validate();
        
        // Validate our fields.
        if (StringUtils.isBlank(adminUser)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "adminUser");
            throw new TapisRuntimeException(msg);
        }
        if (StringUtils.isBlank(adminPassword)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "service");
            throw new TapisRuntimeException(msg);
        }
    }
    
    public String getAdminUser() {
        return adminUser;
    }
    public void setAdminUser(String adminUser) {
        this.adminUser = adminUser;
    }
    public String getAdminPassword() {
        return adminPassword;
    }
    public void setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
    }
}
