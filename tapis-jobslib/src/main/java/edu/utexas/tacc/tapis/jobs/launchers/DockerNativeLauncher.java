package edu.utexas.tacc.tapis.jobs.launchers;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class DockerNativeLauncher 
 extends AbstractJobLauncher
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(DockerNativeLauncher.class);

    // Regex to test docker container id.
    private static final Pattern _nonEmptyAlphaNumeric = Pattern.compile("[a-zA-Z0-9]+");
    private static final Pattern _wsDelimitedWords = Pattern.compile("\\s+");

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public DockerNativeLauncher(JobExecutionContext jobCtx)
     throws TapisException
    {
        // Create and populate the docker command.
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* launch:                                                                */
    /* ---------------------------------------------------------------------- */
    @Override
    public void launch() throws TapisException
    {
        // Throttling adds a randomized delay on heavily used hosts.
        throttleLaunch();
        
        // Subclasses can override default implementation.
        String cmd = getLaunchCommand();
        
        // Log the command we are about to issue.
        if (_log.isDebugEnabled()) 
            _log.debug(MsgUtils.getMsg("JOBS_SUBMIT_CMD", getClass().getSimpleName(), 
                                       _job.getUuid(), cmd));
        
        // Start the container.
        var runCmd     = _jobCtx.getExecSystemTapisSSH().getRunCommand();
        int exitStatus = runCmd.execute(cmd);
        runCmd.logNonZeroExitCode();
        String result  = runCmd.getOutAsString();
        if (StringUtils.isBlank(result)) result = "";
        
        // Exit code fix up.
        if (exitStatus == -1  && !StringUtils.isBlank(result)) {
            // Maybe it actually did work but the networking code couldn't retrieve the proper exit
            // code.  If the result is a 64 character string of alphanumerics, we assume the launch 
            // was successful and reset the exitStatus to zero.
            String temp = result.trim();
            if (temp.length() == 64 && _nonEmptyAlphaNumeric.matcher(temp).matches()) {
                if (_log.isWarnEnabled()) {
                    String msg = MsgUtils.getMsg("JOBS_LAUNCH_EXITCODE_FIXUP", getClass().getSimpleName(), 
                                                 _job.getUuid(), exitStatus);
                    _log.warn(msg);
                }
                // Let's go forward as if we got a zero return code.
                exitStatus = 0;
            }
        }
        
        // Inspect the actual or fixed up exit code.
        String cid = UNKNOWN_CONTAINER_ID;
        if (exitStatus == 0) {
            cid = result.trim();
            if (StringUtils.isBlank(cid)) cid = UNKNOWN_CONTAINER_ID;
               else cid = extractCID(cid);
            if (_log.isDebugEnabled()) {
                String msg = MsgUtils.getMsg("JOBS_SUBMIT_RESULT", getClass().getSimpleName(), 
                                             _job.getUuid(), cid, exitStatus);
                _log.debug(msg);
            }
        } else {
            // Our one chance at launching the container failed with a non-communication
            // error, which we assume is unrecoverable so we abort the job now.
            String msg = MsgUtils.getMsg("JOBS_SUBMIT_ERROR", getClass().getSimpleName(), 
                                         _job.getUuid(), cmd, result, exitStatus);
            throw new TapisException(msg);
        }

        // Save the container id or the unknown id string.
        _jobCtx.getJobsDao().setRemoteJobId(_job, cid);
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getRemoteIdCommand:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Get the remote command that would return the docker container id.
     * Not currently called. 
     * @return the command that returns the container id
     */
    private String getRemoteIdCommand()
    {
        return JobExecutionUtils.getDockerCidCommand(_job.getUuid());
    }
    
    /* ---------------------------------------------------------------------- */
    /* extractCID:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Get the last whitespace delimited word in a result string with any quotes 
     * removed.  This should extract just the container id from any text docker 
     * dumps to stdout screen on a docker run call.
     * 
     * @param s non-null, non-empty, non-whitespace-only string
     * @return the last word without any single quotes
     */
    private String extractCID(String s)
    {
        // Get rid of single quotes.
        s = s.replace("'", "");
        String[] array = _wsDelimitedWords.split(s);
        if (array.length == 0) return UNKNOWN_CONTAINER_ID;
        
        // Only return a well-formed CID.
        var cid = array[array.length-1];
        if (cid.length() == 64 && _nonEmptyAlphaNumeric.matcher(cid).matches())
            return cid;
        return UNKNOWN_CONTAINER_ID;
    }
}
