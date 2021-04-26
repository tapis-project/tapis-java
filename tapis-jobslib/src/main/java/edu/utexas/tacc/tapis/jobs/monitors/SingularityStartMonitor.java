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
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

public class SingularityStartMonitor 
 extends AbstractSingularityMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityStartMonitor.class);
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityStartMonitor(JobExecutionContext jobCtx, MonitorPolicy policy)
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
        PsStartInfo psInfo = extractInstanceInfo(result);
        
        // We should always have found the sinit record, which represents the 
        // process the singularity instance start command spawned.
        if (psInfo.sinit == null) {
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_MISSING_SINIT", _job.getUuid(),
                                         _job.getRemoteJobId());
            _log.error(msg);
            return JobRemoteStatus.EMPTY;
        }
        
        // If there is a startscript process, we assume it's active even
        // though it could be suspended (stopped) or even a zombie.
        if (psInfo.startscript != null) return JobRemoteStatus.ACTIVE;
        
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
    /* cleanUpRemoteJob:                                                      */
    /* ---------------------------------------------------------------------- */
    protected void cleanUpRemoteJob() 
    {
        // Best effort, no noise.
        try {
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
                    return;
                }
            
            // We should have gotten something.
            if (StringUtils.isBlank(result)) return;
            
            // Extract records of interest from the results.
            PsStartInfo psInfo = extractInstanceInfo(result);
            
            // We should always have found the sinit record, which represents the 
            // process the singularity instance start command spawned.
            if (psInfo.sinit == null) {
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_MISSING_SINIT", _job.getUuid(),
                                             _job.getRemoteJobId());
                _log.warn(msg);
                return;
            }
            
            // If there is no startscript process, we remove the instance.
            if (psInfo.startscript == null) removeContainer(runCmd);
              else if (_log.isDebugEnabled()) {
                  _log.debug(MsgUtils.getMsg("JOBS_SINGULARITY_KEEPING_CONTAINER", 
                             _job.getUuid(), psInfo.sinit.pid, 
                             psInfo.startscript.pid, psInfo.startscript.ppid));
              }
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_SINGULARITY_CLEAN_UP_ERROR", _job.getUuid(),
                                         _job.getRemoteJobId());
            _log.error(msg, e);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeContainer:                                                       */
    /* ---------------------------------------------------------------------- */
    private void removeContainer(TapisRunCommand runCmd) 
    {
        // Get the command text to terminate this job's singularity instance.
        String cmd = JobExecutionUtils.SINGULARITY_STOP + _job.getUuid();
        
        // Stop the instance.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                String execSysId = null;
                try {execSysId = _jobCtx.getExecutionSystem().getId();} catch (Exception e1) {}
                String msg = MsgUtils.getMsg("JOBS_SINGULARITY_RM_CONTAINER_ERROR", 
                                             _job.getUuid(), execSysId, result, cmd);
                _log.error(msg, e);
                return;
            }
        
        // Record the successful removal of the instance.
        if (_log.isDebugEnabled()) 
           _log.debug(MsgUtils.getMsg("JOBS_SINGULARITY_INSTANCE_REMOVED",_job.getUuid()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* extractInstanceInfo:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Extract process information for remote processes associated with this
     * job.  The job's remoteJobId is the PID returned by "singularity instance start"
     * command and should always be present in the monitoring results.  
     * 
     * @param results raw results from remote ps command
     * @return the parsed records of interest for this job
     */
    private PsStartInfo extractInstanceInfo(String results)
    {
        // Example list call:
        //  singularity instance list XXX
        //     - 624785 is returned for example below
        //
        // Monitor command:
        //  ps --no-headers --sort=pid -eo pid,ppid,stat,euser,cmd
        //
        // Example ps result records:
        //
        //  624784    2286 Ssl  rcardone Singularity instance: rcardone [XXX]
        //  624785  624784 Sl   rcardone sinit
        //  624799       2 S<   root     [loop1]
        //  624807  624785 S    rcardone /bin/sh /.singularity.d/startscript
        //  624810  624807 Sl   rcardone java -cp /usr/local/bin/testapps.jar edu.utexas.tacc.testapps.tapis.SleepSeconds 120
        
        // Result object.
        var info = new PsStartInfo();
        final String instanceSearch = "[" + _job.getUuid() + "]";
        String startscriptPid = null;
        
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
            if (info.instance == null && rest.endsWith(instanceSearch)) {
                info.instance = new PsRecord(pid, ppid, rest);
            } else if (info.sinit == null && pid.equals(_job.getRemoteJobId())) {
                info.sinit = new PsRecord(pid, ppid, rest);
            } else if (info.startscript == null && ppid.equals(_job.getRemoteJobId())) {
                info.startscript = new PsRecord(pid, ppid, rest);
                startscriptPid = pid;  // the process whose absence indicates app termination
            } else if (startscriptPid != null && info.app == null && ppid.equals(startscriptPid)) {
                info.app = new PsRecord(pid, ppid, rest);
            }
        
            // Terminate search early if possible.
            if (info.isComplete()) return info;
        }
        
        return info;
    }
    
    /* ********************************************************************** */
    /*                            PsStartInfo Class                           */
    /* ********************************************************************** */
    /** This class holds information about the processes associated with the 
     * singularity instance of the Tapis job.
     */
    private final static class PsStartInfo
    {
        private PsRecord instance;
        private PsRecord sinit;
        private PsRecord startscript;
        private PsRecord app;
        
        // Have all fields been assigned?
        private boolean isComplete() {
            if (instance == null || sinit == null || startscript == null || app == null)
                return false;
            return true;
        }
    }
}
