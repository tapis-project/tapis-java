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
    private int     adminPort;
    
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
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "adminPassword");
            throw new TapisRuntimeException(msg);
        }
        if (adminPort <= 0) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validate", "adminPort", 
                                         adminPort);
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
    public int getAdminPort() {
        return adminPort;
    }
    public void setAdminPort(int adminPort) {
        this.adminPort = adminPort;
    }
}
