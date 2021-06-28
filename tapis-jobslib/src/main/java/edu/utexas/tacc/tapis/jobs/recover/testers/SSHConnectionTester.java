package edu.utexas.tacc.tapis.jobs.recover.testers;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobRecoveryAbortException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;

public final class SSHConnectionTester 
 extends SSHAuthenticationTester
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SSHConnectionTester.class);
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Set to true the first time the tester parameters are validated.
    private boolean _parmsValidated = false;
    
    /* ********************************************************************** */
    /*                               Constructors                             */
    /* ********************************************************************** */
    /* -----------------------------------------------------------------------*/
    /* constructor:                                                           */
    /* -----------------------------------------------------------------------*/
    public SSHConnectionTester(JobRecovery jobRecovery)
    {
        super(jobRecovery);
    }
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* canUnblock:                                                            */
    /* ---------------------------------------------------------------------- */
    @Override
    public int canUnblock(Map<String, String> testerParameters) 
     throws JobRecoveryAbortException
    {
        // Validate the tester parameters. 
        // An exception can be thrown here.
        validateTesterParameters(testerParameters);
        
        // Try to connect.
        boolean canConnectChannel = canEstablishConnection();
        
        // Return whether is the system is marked available.
        if (canConnectChannel) return DEFAULT_RESUBMIT_BATCHSIZE;
        else return NO_RESUBMIT_BATCHSIZE;
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* validateTesterParameters:                                              */
    /* ---------------------------------------------------------------------- */
    /** Validate the tester parameters for existence and type. If validation 
     * fails on the first attempt, then the exception thrown will cause the
     * recovery abort and all its jobs to be failed. If validation success
     * on the first attempt, then we skip validation in this object from now on.
     * 
     * @param testerParameters the test parameter map
     * @throws JobRecoveryAbortException on validation error
     */
    protected void validateTesterParameters(Map<String, String> testerParameters)
     throws JobRecoveryAbortException
    {
        // No need to validate more than once.
        if (_parmsValidated) return;
        
        // Validate superclass.
        super.validateTesterParameters(testerParameters);
        
        // We're good if we get here.
        _parmsValidated = true;
    }
}
