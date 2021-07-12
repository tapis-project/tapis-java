package edu.utexas.tacc.tapis.jobs.monitors;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.exceptions.JobMonitorResponseException;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.JobRemoteStatus;
import edu.utexas.tacc.tapis.jobs.monitors.parsers.SlurmStatusType;
import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

/** Slurm job monitoring class.
 *
 *  Response is determined using either the primary monitoring squeue command:
 *     squeue --noheader -O 'jobid,state,exit_code' -j${JOBID} 2>/dev/null
 *
 *     Example of response returned by squeue:
 *          "4213134             RUNNING                   0"
 *   
 *  Or by the secondary, post execution monitoring sacct command:
 *     sacct -p -o 'JobID,State,ExitCode' -n -j ${JOBID}
 *       
 *     Example of response returned by sacct:
 *          "<jobid>|<state>|<exit_code>|"
 *
 * NOTE: If info is no longer available using squeue then squeue responds on stderr with:
 *           "slurm_load_jobs error: Invalid job id specified"
 *       This is why stderr is redirected to /dev/null.
 */
public final class SlurmMonitor 
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SlurmMonitor.class);
    
    // Placeholder string.
    private static final String PLACEHOLDER = "${JOBID}";
    
    // Active query command.
    private static final String ACTIVE_CMD = 
        "squeue --noheader -O 'jobid,statecompact,exit_code' -j ${JOBID} 2>/dev/null";
    
    // Active command response parser.
    private static final Pattern _spaceDelimited = 
        Pattern.compile("\\s*(\\S+)\\s+(\\S+)\\s+(\\S+)\\s*");
    
    // Inactive query command.
    private static final String INACTIVE_CMD = 
        "sacct -p -o 'JobID,State,ExitCode' -n -j ${JOBID}";
    
    // Inactive command response splitter.
    // Need to quote the pipe metacharacter; alternate form is "\\Q|\\E".
    private static final Pattern _pipeSplitter = Pattern.compile(Pattern.quote("|"));
    
    // Empty parser response.
    private static final ParsedStatusResponse EMPTY_PARSED_RESP = 
        new ParsedStatusResponse("", "", "");

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The response from the current query command or null.
    private ParsedStatusResponse _parsedStatusResponse;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected SlurmMonitor(JobExecutionContext jobCtx, MonitorPolicy policy) 
    {
        super(jobCtx, policy);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getExitCode:                                                           */
    /* ---------------------------------------------------------------------- */
    @Override
    public String getExitCode() {
        if (_parsedStatusResponse == null) return null;
        return _parsedStatusResponse.getExitCode();
    }

    /* ---------------------------------------------------------------------- */
    /* queryRemoteJob:                                                        */
    /* ---------------------------------------------------------------------- */
    @Override
    protected JobRemoteStatus queryRemoteJob(boolean active) throws TapisException 
    {
        // Sanity check--we can't do much without the remote job id.
        if (StringUtils.isBlank(_job.getRemoteJobId())) {
            String msg = MsgUtils.getMsg("JOBS_MISSING_REMOTE_JOB_ID", _job.getUuid());
            throw new JobException(msg);
        }
        
        // Reset the response.
        _parsedStatusResponse = null;
        
        // Get the command object.
        var runCmd = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        
        // Get the command text for this job's container.
        String cmd;
        if (active) cmd = ACTIVE_CMD;
          else cmd = INACTIVE_CMD;
        
        // Substitute the actual remote id.
        cmd = cmd.replace(PLACEHOLDER, _job.getRemoteJobId());
        if (_log.isDebugEnabled())
            _log.debug(MsgUtils.getMsg("JOBS_MONITOR_COMMAND", _job.getUuid(), 
                                       _jobCtx.getExecutionSystem().getHost(), 
                                       _jobCtx.getExecutionSystem().getPort(), cmd));
        
        // Query the container.
        String result = null;
        try {
            int rc = runCmd.execute(cmd);
            runCmd.logNonZeroExitCode();
            result = runCmd.getOutAsString();
        }
        catch (Exception e) {
            _log.error(e.getMessage(), e);
            return JobRemoteStatus.NULL;
        }
        
        // We should have gotten something.
        if (StringUtils.isBlank(result)) return JobRemoteStatus.EMPTY;
        
        // Parse the non-null result.
        _parsedStatusResponse = parseResponse(result, active);
        
        // If the state info is missing, the job isn't running (or so we think).
        if (StringUtils.isEmpty(_parsedStatusResponse.getStatus())) {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_NO_STATUS", 
                                         getClass().getSimpleName(),
                                         _job.getUuid(), _job.getRemoteJobId());
            _log.warn(msg);
            return JobRemoteStatus.EMPTY;
        }
        
        // Canonicalize the status.  It should not have embedded spaces,  
        // but we replicate this constraint from Agave out of paranoia.
        String firstStatusWord = 
            StringUtils.substringBefore(_parsedStatusResponse.getStatus(), " ").toUpperCase();
        
        // Get the typed status.
        SlurmStatusType statusType;
        try {statusType = SlurmStatusType.valueOf(firstStatusWord);}
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_UNKNOWN_RESPONSE",
                                         getClass().getSimpleName(),
                                         _parsedStatusResponse.getJobId(),
                                         firstStatusWord,
                                         _job.getUuid());
            throw new JobMonitorResponseException(msg, e);
        }

        // Are we still waiting in the HPC queue?
        if (statusType.isQueued()) return JobRemoteStatus.QUEUED;
        
        // Return right away if the job completed.
        if (statusType.isCompleted()) return JobRemoteStatus.DONE;
            
        // Return right away if the job is active.
        if (statusType.isActive()) return JobRemoteStatus.ACTIVE;
        
        // Count slurm-paused as running.
        if (statusType.isPaused()) return JobRemoteStatus.ACTIVE;
        
        // If the job is in an unrecoverable state, throw the exception so the job is cleaned up.
        if (statusType.isUnrecoverable()) {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_UNRECOVERABLE_RESPONSE", 
                                         getClass().getSimpleName(), _parsedStatusResponse.getJobId(), 
                                         statusType.name(), _parsedStatusResponse.getExitCode(),
                                         _job.getUuid());
            _log.warn(msg);
            
            // Update the finalMessage field in the jobCtx to reflect this status. 
            updateFinalMessage(_parsedStatusResponse);
            return JobRemoteStatus.FAILED;
        }
        
        // Failures.
        if (statusType.isFailed()) {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_FAILURE_RESPONSE", 
                                         getClass().getSimpleName(), _parsedStatusResponse.getJobId(), 
                                         statusType.name(), _parsedStatusResponse.getExitCode(),
                                         _job.getUuid());
            _log.warn(msg);
            
            // Update the finalMessage field in the jobCtx to reflect this status. 
            updateFinalMessage(_parsedStatusResponse);
            return JobRemoteStatus.FAILED;
        }
        
        // We shouldn't get here since all slurm states are accounted for 
        // in the above conditionals, but if we do get here we note it.
        String msg = MsgUtils.getMsg("JOBS_MONITOR_UNKNOWN_RESPONSE", 
                                     getClass().getSimpleName(),
                                     _parsedStatusResponse.getJobId(),
                                     _parsedStatusResponse.getStatus().toUpperCase(),
                                     _job.getUuid());
        _log.warn(msg);
        return JobRemoteStatus.DONE;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* parseResponse:                                                         */
    /* ---------------------------------------------------------------------- */
    /** The response should come back as space or '|' delimited string like:
     * 
     *          "{@code <job_id> <state> <exit_code>}".
     *
     * If the response is null or blank then we return null.
     * Otherwise we return parsed response object which will be the empty object
     * if parsing was unsuccessful.
     * 
     * @param schedulerResponse
     *            the raw, non-null response from the scheduler. Should be in format:
     *            
     *            "{@code <job_id>   <state>   <exit_code>}" or
     *            "{@code <job_id>|<state>|<exit_code>|}"
     * @return a parsed response object or null if the response is null or blank
     */
    private ParsedStatusResponse parseResponse(String schedulerResponse, boolean active)
    {
        // Get rid of any leading or trailing whitespace.
        var trimmedResponse = schedulerResponse.trim();
        if (StringUtils.isBlank(trimmedResponse)) {
            String msg = MsgUtils.getMsg("JOBS_MONITOR_NO_RESPONSE");
            _log.error(msg);
            return null;
        }

        // Active responses are space delimited, inactive ones are '|' delimited.
        ParsedStatusResponse resp;
        if (active) {
           // ----------------- Active Job -------------------
           // Parse the active command's response. 
           trimmedResponse = JobUtils.getLastLine(trimmedResponse);
           var matcher = _spaceDelimited.matcher(trimmedResponse);
           var matches = matcher.matches();
           if (!matches) {
               if (_log.isDebugEnabled()) {
                   String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", trimmedResponse);
                   _log.debug(msg);
               }
               return EMPTY_PARSED_RESP;
           }
           var groupCount = matcher.groupCount();
           if (groupCount != 3) {
               if (_log.isDebugEnabled()) {
                   String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", trimmedResponse);
                   _log.debug(msg);
               }
               return EMPTY_PARSED_RESP;
           }
          
          // Create response.
          resp = new ParsedStatusResponse(matcher.group(1), matcher.group(2), matcher.group(3));
        } 
        else {
          // ----------------- Inactive Job -----------------
          // Split the inactive command's response on pipe characters.
          // The results could look like this (note the embedded newline):
          //
          //    65|FAILED|127:0|\n65.batch|FAILED|127:0|
          //
          trimmedResponse = removeBannerFromInactive(trimmedResponse);
          var parts = _pipeSplitter.split(trimmedResponse);
          if (parts.length < 3) {
              if (_log.isDebugEnabled()) {
                  String msg = MsgUtils.getMsg("JOBS_MONITOR_INVALID_RESPONSE", trimmedResponse);
                  _log.debug(msg);
              }
              return EMPTY_PARSED_RESP;
          }
        
          // Create response removing any whitespace.
          resp = new ParsedStatusResponse(parts[0].trim(), parts[1].trim(), parts[2].trim());
        }
      
        return resp;
    }
    
    /* ---------------------------------------------------------------------- */
    /* updateFinalMessage:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Helper method that updates the finalMessage field with useful messaging 
     * in the jobCtx for certain failure scenarios.  The lastMessage field in 
     * the db will be updated at the end of the job to reflect the finalMessage,
     * if finalMessage is not null. 
     * 
     * @param parsedResponse monitoring response object for failed jobs
     */
    private void updateFinalMessage(ParsedStatusResponse parsedResponse) {
        String rc = StringUtils.isBlank(parsedResponse.getExitCode()) ? 
                                        "unknown" : parsedResponse.getExitCode();
        String finalMessage = MsgUtils.getMsg("APPS_USER_APP_FAILURE", 
                                              parsedResponse.getStatus(), 
                                              rc); 
        _job.getJobCtx().setFinalMessage(finalMessage);
    }
    
    /* ---------------------------------------------------------------------- */
    /* removeBannerFromInactive:                                              */
    /* ---------------------------------------------------------------------- */
    /** Remove any gorp that might appear before the actual command response.
     * We can't use JobUtils.getLastLine(response) here because the response 
     * might container more than one line.  Instead we look for the first place
     * the job id appears with a pipe character immediately following it. 
     * 
     * @param response the raw response from an inactive monitor query
     * @return the possibly decluttered response
     */
    private String removeBannerFromInactive(String response)
    {
        String start = _job.getRemoteJobId() + "|";
        int index = response.indexOf(start);
        if (index < 1) return response;
          else return response.substring(index); 
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    private static final class ParsedStatusResponse 
    {
        // Each field can be null, the empty string
        // or an actual text value.
        private String jobId;
        private String status;
        private String exitCode;
        
        // Constructor.
        private ParsedStatusResponse(String j, String s, String e)
        {jobId = j; status = s; exitCode = e;}

        // Accessors.
        private String getJobId() {return jobId;}
        private String getStatus() {return status;}
        private String getExitCode() {return exitCode;}
    }
}
