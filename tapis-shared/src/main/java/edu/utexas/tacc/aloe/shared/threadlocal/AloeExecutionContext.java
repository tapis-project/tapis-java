package edu.utexas.tacc.aloe.shared.threadlocal;

import javax.sql.DataSource;

import edu.utexas.tacc.aloe.shared.exceptions.AloeException;

public interface AloeExecutionContext 
{
    /** Retrieve the tenant corresponding to the current tenant id.
     * @return a edu.utexas.tacc.aloe.tenants.model.Tenant object
     */
    Object getTenant() throws AloeException;
    
    /** Retrieve the tenant's base url without requiring the caller
     * to have access to the Tenant class.
     * @return the current tenant's base url as a string
     */
    String getTenantBaseUrl() throws AloeException;
    
    /** Get the source for connections to our database.
     * @return the database connection source
     */
    DataSource getDataSource() throws AloeException;
    
    // ------------------- Notifications -------------------
    /** Get the notifications queue type. */
    String getNotifType();
    
    /** Get the notification queue. */
    String getNotifQueue();
    
    /** Get the notification host. */
    String getNotifHost();
    
    /** Get the notification port. */
    int getNotifPort();
    
    /** Get the notification retry queue. */
    String getNotifRetryQueue();
    
    /** Get the notification topic. */
    String getNotifTopic();
    
}
