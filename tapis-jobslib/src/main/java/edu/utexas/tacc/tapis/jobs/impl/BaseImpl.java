package edu.utexas.tacc.tapis.jobs.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobsDao;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

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
}
