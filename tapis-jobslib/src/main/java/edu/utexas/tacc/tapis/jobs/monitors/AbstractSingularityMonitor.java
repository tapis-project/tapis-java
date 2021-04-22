package edu.utexas.tacc.tapis.jobs.monitors;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.monitors.policies.MonitorPolicy;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.ssh.system.TapisRunCommand;

public abstract class AbstractSingularityMonitor 
 extends AbstractJobMonitor
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractSingularityMonitor.class);

    // Split on new lines.
    protected static final Pattern _newLinePattern = Pattern.compile("\n");
    
    // Result parser for ps command:
    //   ps --no-headers --sort=pid -eo pid,ppid,stat,euser,cmd
    //
    // This regex disregards leading whitespace, puts the first word in group 1, 
    // the second word in group 2 and the rest in group 3.  Example input that 
    // looks like this:
    //
    //   " 624784    2286 Ssl  rcardone Singularity instance: rcardone [XXX]"
    //
    // Gets grouped like this:
    //
    //   group count: 3
    //   0:  624784    2286 Ssl  rcardone Singularity instance: rcardone [XXX]
    //   1: 624784
    //   2: 2286
    //   3: Ssl  rcardone Singularity instance: rcardone [XXX]
    protected static final Pattern _psPattern = Pattern.compile("\\s*(\\S+)\\s+(\\S+)\\s+(.+)");
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // The application return code as reported by docker.
    protected String _exitCode;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractSingularityMonitor(JobExecutionContext jobCtx, MonitorPolicy policy) 
    {
        super(jobCtx, policy);
    }

    /* ---------------------------------------------------------------------- */
    /* readExitCodeFile:                                                      */
    /* ---------------------------------------------------------------------- */
    protected String readExitCodeFile(TapisRunCommand runCmd) 
    {
        // Initialize output to default to no error.
        String exitcode = SUCCESS_RC;
        
        // Create the command that returns the exit code contents if the
        // file exists in the job's output directory.  There's not much we
        // can do if we encounter an error here.
        String cmd = null;
        try {
            var fm = _jobCtx.getJobFileManager();
            var filepath = fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_EXITCODE_FILE);
            cmd = "cat " + filepath;
        } catch (Exception e) {
            _log.error(e.getMessage(), e);
            return exitcode;
        }
        
        // Issue the command.
        String result = null;
        try {result = runCmd.execute(cmd);}
            catch (Exception e) {
                _log.error(e.getMessage(), e);
                return exitcode;
            }
        
        // See if we even found the file.
        if (StringUtils.isBlank(result)) return exitcode;
        result = result.trim();
        if (result.isEmpty() || result.startsWith("cat") || result.contains("No such")) 
           return exitcode;
        
        // We assign exitcode as long as the result is an integer.
        try {Integer.valueOf(result); exitcode = result;}
            catch (Exception e) {}
        
        return exitcode;
    }
    
    /* ********************************************************************** */
    /*                             PsRecord Class                             */
    /* ********************************************************************** */
    /** Record to hold parsed result of ps command described above. */
    protected final static class PsRecord
    {
        protected String  pid;
        protected String  ppid; 
        protected String  rest;
        
        // Constructor.
        protected PsRecord(String pid, String ppid, String rest) {
            this.pid = pid; this.ppid = ppid; this.rest = rest;
        }
    }
}
