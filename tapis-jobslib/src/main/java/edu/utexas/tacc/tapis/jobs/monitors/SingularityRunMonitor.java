package edu.utexas.tacc.tapis.jobs.monitors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

public class SingularityRunMonitor 
 extends AbstractSingularityMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunMonitor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
    {super(jobCtx, policy);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getExitCode:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Return the application exit code as reported by docker.  If no exit
     * code has been ascertained, null is returned. 
     * 
     * @return the application exit code or null
     */
    @Override
    public String getExitCode() {return _exitCode;}
    
    /* ---------------------------------------------------------------------- */
    /* monitorQueuedJob:                                                      */
    /* ---------------------------------------------------------------------- */
    @Override
    public void monitorQueuedJob() throws TapisException
    {
        // The queued state is a no-op for forked jobs.
    }

    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    /** Query the status of the job's container.  When the container terminates,
     * this method captures the application's exit code and makes it available
     * to the caller.
     * 
     */
    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException
    {
        // There's no difference between the active and inactive singularity queries, 
        // so there's no point in issuing a second (inactive) query if the first
        // one did not return a result.
        if (!active) return JobRemoteStatus.NULL;
        
        // Sanity check--we can't do much without the remote job id.
        if (StringUtils.isBlank(_job.getRemoteJobId())) {
            String msg = MsgUtils.getMsg("JOBS_MISSING_REMOTE_JOB_ID", _job.getUuid());
            throw new JobException(msg);
        }
        
        // Get the ssh connection used by this job to
        // communicate with the execution system.
        var conn = _jobCtx.getExecSystemConnection();
        
        // Get the command object.
        var runCmd = new TapisRunCommand(_jobCtx.getExecutionSystem(), conn);
        
        // Get the command text for this job's container.
        String cmd = JobExecutionUtils.SINGULARITY_START_MONITOR;
        
        // Query the container.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                return JobRemoteStatus.NULL;
            }
        
        // We should have gotten something.
        if (StringUtils.isBlank(result)) return JobRemoteStatus.EMPTY;
        
        // Extract records of interest from the results.
        PsRunInfo psInfo = extractInstanceInfo(result);
        
        // If the parent isn't running we can assume application execution terminated,
        // though the parent could be suspended (stopped) or even a zombie.
        if (psInfo.parent != null) return JobRemoteStatus.ACTIVE;
        
        // Remove the container from the execution system.
        removeContainer(runCmd);

        // No startscript means the application has terminated.  If possible,
        // let's determine if it failed or succeeded by reading the optional
        // exit code file.  Even if that fails, the exit code is set.
        _exitCode = readExitCodeFile(runCmd);
        if (!SUCCESS_RC.equals(_exitCode)) return JobRemoteStatus.FAILED;
          else return JobRemoteStatus.DONE;
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisRunCommand runCmd) 
     throws TapisServiceConnectionException, TapisImplException
    {
        // Get the command text to terminate this job's singularity instance.
        String cmd = JobExecutionUtils.SINGULARITY_STOP + _job.getUuid();
        
        // Stop the instance.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                var execSysId = _jobCtx.getExecutionSystem().getId();
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RM_CONTAINER_ERROR", 
                                             _job.getUuid(), execSysId, result, cmd);
                _log.error(msg, e);
            }
    }
    
    /* ---------------------------------------------------------------------- */
    /* extractInstanceInfo:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Extract process information for remote processes associated with this
     * job.  The job's remoteJobId is the PID returned by "singularity run ... &"
     * command and should always be present in the monitoring results.  
     * 
     * @param results raw results from remote ps command
     * @return the parsed records of interest for this job
     */
    private PsRunInfo extractInstanceInfo(String results)
    {
        // Example run call:
        //  singularity run XXX.sif &
        //     - 898024 is returned for example below
        //
        // Monitor command:
        //  ps --no-headers --sort=pid -eo pid,ppid,stat,euser,cmd
        //
        // Example ps result records:
        //
        //  898024    2286 Sl   rcardone Singularity runtime parent
        //  898044  898024 S    rcardone /bin/sh /.singularity.d/runscript
        //  898057       2 S<   root     [loop1]
        //  898066  898044 Sl   rcardone java -cp /usr/local/bin/testapps.jar edu.utexas.tacc.testapps.tapis.SleepSeconds 120
        
        // Result object.
        var info = new PsRunInfo();
        String runscriptPid = null;
        
        // Split on newlines.
        String[] records = _newLinePattern.split(results);
      
        // Process each record.
        for (var r : records) {
            var m = _psPattern.matcher(r);
            var b = m.matches();
            if (!b) {
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_PS_PARSE_WARN", _job.getUuid(), r);
                _log.warn(msg);
                continue;
            }
            
            // Unpack the parsed results.
            var pid  = m.group(1);
            var ppid = m.group(2); // never missing
            var rest = m.group(3);
            
            // Try to fill in each of the info fields. Note that the ps monitoring command
            // returns records in ascending pid order.  This causes records to be returned 
            // in the order shown above.  This code below doesn't depend on this ordering,
            // but does test records in the same order.
            if (info.parent == null && pid.equals(_job.getRemoteJobId())) {
                info.parent = new PsRecord(pid, ppid, rest);
            } else if (info.runscript == null && ppid.equals(_job.getRemoteJobId())) {
                info.runscript = new PsRecord(pid, ppid, rest);
                runscriptPid = pid;  // the process whose absence indicates app termination
            } else if (runscriptPid != null && info.app == null && ppid.equals(runscriptPid)) {
                info.app = new PsRecord(pid, ppid, rest);
            }
        
            // Terminate search early if possible.
            if (info.isComplete()) return info;
        }
        
        return info;
    }
    
    /* ********************************************************************** */
    /*                            PsRunInfo Class                             */
    /* ********************************************************************** */
    /** This class holds information about the processes associated with the 
     * singularity run of the Tapis job.
     */
    private final static class PsRunInfo
    {
        private PsRecord parent;
        private PsRecord runscript;
        private PsRecord app;
        
        // Have all fields been assigned?
        private boolean isComplete() {
            if (parent == null || runscript == null || app == null) return false;
              else return true;
        }
    }
}
