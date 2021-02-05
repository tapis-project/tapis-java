package edu.utexas.tacc.tapis.jobs.recover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobRecovery;
import edu.utexas.tacc.tapis.jobs.recover.testers.ApplicationTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.AuthenticationTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.ConnectionTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.DatabaseTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.JobSuspendedTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.QuotaTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.SystemTester;
import edu.utexas.tacc.tapis.jobs.recover.testers.TransmissionTester;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Singleton factory class that returns recovery testers.
 * 
 * @author rcardone
 */
public class RecoverTesterFactory 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(RecoverTesterFactory.class);

    /* **************************************************************************** */
    /*                                    Fields                                    */
    /* **************************************************************************** */
    private static RecoverTesterFactory _instance;
    
    /* **************************************************************************** */
    /*                                 Constructors                                 */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* constructor:                                                                 */
    /* ---------------------------------------------------------------------------- */
    private RecoverTesterFactory() {}
    
    /* **************************************************************************** */
    /*                                 Public Methods                               */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getInstance:                                                                 */
    /* ---------------------------------------------------------------------------- */
    public static RecoverTesterFactory getInstance()
    {
        // Serialize instance creation without incurring
        // synchronization overhead most of the time.
        if (_instance == null) {
            synchronized (RecoverTesterFactory.class) {
                if (_instance == null) _instance = new RecoverTesterFactory(); 
            }
        }
        return _instance;
    }
    
    /* ---------------------------------------------------------------------- */
    /* getTester:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Return a new tester instance based on the tester type.
     * 
     * @param testerType the type of tester needed
     * @return a new tester instance
     * @throws JobException on unknown tester types
     */
    public RecoverTester getTester(JobRecovery jobRecovery) 
     throws TapisRuntimeException
    {
        // Return the custom tester object for each tester type.
        RecoverTesterType testerType = jobRecovery.getTesterType();
        switch (testerType)
        {
            // Each tester type has its own tester class.
            case DEFAULT_SYSTEM_AVAILABLE_TESTER:
                return new SystemTester(jobRecovery);
            case DEFAULT_APPLICATION_TESTER:
                return new ApplicationTester(jobRecovery);
            case DEFAULT_CONNECTION_TESTER:
                return new ConnectionTester(jobRecovery);
            case DEFAULT_TRANSMISSION_TESTER:
                return new TransmissionTester(jobRecovery);
            case DEFAULT_DATABASE_TESTER:
                return new DatabaseTester(jobRecovery);
            case DEFAULT_QUOTA_TESTER:
                return new QuotaTester(jobRecovery);
            case DEFAULT_JOB_SUSPENDED_TESTER:
                return new JobSuspendedTester(jobRecovery);
            case DEFAULT_AUTHENTICATION_TESTER:
                return new AuthenticationTester(jobRecovery);
        
            default:
            {
                // This can only happen if we forget to support a new tester type.
                String msg = MsgUtils.getMsg("JOBS_RECOVERY_UNKNOWN_TESTERTYPE", 
                                             testerType, jobRecovery.getId(), 
                                             jobRecovery.getTenantId());
                _log.error(msg);
                throw new TapisRuntimeException(msg);
            }
        }
    }
}
