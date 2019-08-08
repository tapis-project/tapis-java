package edu.utexas.tacc.tapis.sharedq;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class QueueManagerParms 
{
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(QueueManagerParms.class);
    
    // RabbitMQ configuration fields.
    private String  instanceName; // Name of program instance
    private String  queueUser;
    private String  queuePassword;
    private String  queueHost;
    private int     queuePort;
    private boolean queueSSLEnabled;
    private boolean queueAutoRecoveryEnabled;
    
    // Validation method should be called before first parameter use.
    public void validate() throws TapisException
    {
        if (StringUtils.isBlank(instanceName)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "instanceName");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(queueUser)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "queueUser");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (StringUtils.isBlank(queueHost)) {
            String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "queueHost");
            _log.error(msg);
            throw new TapisException(msg);
        }
        if (queuePort == 0) {
            String msg = MsgUtils.getMsg("TAPIS_INVALID_PARAMETER", "validate", "queuePort", 
                                         queuePort);
            _log.error(msg);
            throw new TapisException(msg);
        }
    }
    
    // Accessors.
    public String getInstanceName() {
        return instanceName;
    }
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
    public String getQueueUser() {
        return queueUser;
    }
    public void setQueueUser(String queueUser) {
        this.queueUser = queueUser;
    }
    public String getQueuePassword() {
        return queuePassword;
    }
    public void setQueuePassword(String queuePassword) {
        this.queuePassword = queuePassword;
    }
    public String getQueueHost() {
        return queueHost;
    }
    public void setQueueHost(String queueHost) {
        this.queueHost = queueHost;
    }
    public int getQueuePort() {
        return queuePort;
    }
    public void setQueuePort(int queuePort) {
        this.queuePort = queuePort;
    }
    public boolean isQueueSSLEnabled() {
        return queueSSLEnabled;
    }
    public void setQueueSSLEnabled(boolean queueSSLEnabled) {
        this.queueSSLEnabled = queueSSLEnabled;
    }
    public boolean isQueueAutoRecoveryEnabled() {
        return queueAutoRecoveryEnabled;
    }
    public void setQueueAutoRecoveryEnabled(boolean queueAutoRecoveryEnabled) {
        this.queueAutoRecoveryEnabled = queueAutoRecoveryEnabled;
    }
}
