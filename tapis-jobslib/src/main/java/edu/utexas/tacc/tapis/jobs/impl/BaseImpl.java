package edu.utexas.tacc.tapis.jobs.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobEventsDao;
import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.security.client.SKClient;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceClients;

abstract class BaseImpl
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(BaseImpl.class);
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // We share all dao's among all instances of this class.
    private static JobsDao           _jobsDao;
    private static JobEventsDao       _jobEventsDao;
    
    /* **************************************************************************** */
    /*                             Protected Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getJobsDao:                                                                  */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static JobsDao getJobsDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_jobsDao == null) 
            synchronized (BaseImpl.class) {
                if (_jobsDao == null) _jobsDao = new JobsDao();
           }
            
        return _jobsDao;
    }
    /* ---------------------------------------------------------------------------- */
    /* getJobEventsDao:                                                             */
    /* ---------------------------------------------------------------------------- */
    /** Create the shared dao on first reference.
     * 
     * @return the dao
     * @throws TapisException on error
     */
    protected static JobEventsDao getJobEventsDao() 
     throws TapisException
    {
        // Avoid synchronizing exception for initialization.
        if (_jobEventsDao == null) 
            synchronized (BaseImpl.class) {
                if (_jobEventsDao == null) _jobEventsDao = new JobEventsDao();
           }
            
        return _jobEventsDao;
    }
    /* ---------------------------------------------------------------------------- */
    /* isAdmin:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Check for admin role without throwing an exception.  Log errors and return
     * false if unable to determine admin status.
     * 
     * @param user the user whose authorization is being checked
     * @param tenant the user's tenant
     * @return true if the user's administrator privileges are confirmed, false otherwise
     */
    protected boolean isAdminSafe(String user, String tenant)
    {
        boolean isAdmin = false;
        try {isAdmin = isAdmin(user, tenant);}
            catch (Exception e) {_log.error(e.getMessage(), e);}
        return isAdmin;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* isAdmin:                                                                     */
    /* ---------------------------------------------------------------------------- */
    /** Check for admin role and throw an exception if the check cannot be performed.
     * 
     * @param user the user whose authorization is being checked
     * @param tenant the user's tenant
     * @return true if the user is an administrator, false otherwise
     * @throws TapisException if the check cannot be performed
     */
    protected boolean isAdmin(String user, String tenant) throws TapisException
    {
        // Get the application client for this user@tenant.
        SKClient skClient = null;
        try {
            skClient = ServiceClients.getInstance().getClient(user, tenant, SKClient.class);
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "SK", "getClient", tenant, user);
            throw new TapisException(msg, e);
        }
        
        // Make the SK call.
        boolean isAdmin;
        try {isAdmin = skClient.isAdmin(tenant, user);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "SK", "isAdmin", tenant, user);
                throw new TapisException(msg, e);
            }
        
        return isAdmin;
    }
}
