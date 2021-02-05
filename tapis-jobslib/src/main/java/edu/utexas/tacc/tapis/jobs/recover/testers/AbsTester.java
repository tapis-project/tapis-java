package edu.utexas.tacc.tapis.jobs.recover.testers;

import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.RecoverTester;

public abstract class AbsTester 
 implements RecoverTester
{
    /* **************************************************************************** */
    /*                                  Constants                                   */
    /* **************************************************************************** */
    protected static final int NO_RESUBMIT_BATCHSIZE = 0;
    protected static final int DEFAULT_RESUBMIT_BATCHSIZE = 10;
    
    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    // The recovery job to which this policy will be applied.
    protected final JobRecovery _jobRecovery;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    protected AbsTester( JobRecovery jobRecovery)
    {
        _jobRecovery = jobRecovery;
    }
}
