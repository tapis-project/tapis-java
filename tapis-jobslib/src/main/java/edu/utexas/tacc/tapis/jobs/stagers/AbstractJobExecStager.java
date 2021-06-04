package edu.utexas.tacc.tapis.jobs.stagers;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public abstract class AbstractJobExecStager 
 implements JobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractJobExecStager.class);
    
    // Command buffer initial capacity.
    private static final int INIT_CMD_LEN = 2048;

    // Command line option parser.  This regex captures 3 groups:
    //
    //   0 - the complete value unparsed
    //   1 - the docker option starting with 1 or 2 hypens (-e, --env, etc.)
    //   2 - the value assigned to the option, which may be empty
    //
    // Leading and trailing whitespace is ignored, as is any whitespace between
    // the option and value.  The optional equals sign is also ignored, whether
    // there's whitespace on either side of it or not.
    // (\s=whitespace, \S=not whitespace)
    protected static final Pattern _optionPattern = Pattern.compile("\\s*(--?[^=\\s]*)\\s*=?\\s*(\\S*)\\s*");
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Input parameters
    protected final JobExecutionContext _jobCtx;
    protected final Job                 _job;
    
    // The buffer used to build command file content. 
    protected final StringBuilder       _cmd;

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractJobExecStager(JobExecutionContext jobCtx)
    {
        _jobCtx = jobCtx;
        _job    = jobCtx.getJob();
        
        // Initialize the command file text.
        _cmd = new StringBuilder(INIT_CMD_LEN);
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* stageJob:                                                              */
    /* ---------------------------------------------------------------------- */
    @Override
    public void stageJob() throws TapisException 
    {
        // Create the wrapper script.
        String wrapperScript = generateWrapperScript();
        
        // Create the environment variable definition file.
        String envVarFile = generateEnvVarFile();
        
        // Get the ssh connection used by this job 
        // communicate with the execution system.
        var fm = _jobCtx.getJobFileManager();
        
        // Install the wrapper script on the execution system.
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);
        
        // Install the env variable definition file.
        fm.installExecFile(envVarFile, JobExecutionUtils.JOB_ENV_FILE, JobFileManager.RWRW);
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    /** This method generates the wrapper script content.
     * 
     * @return the wrapper script content
     */
    protected abstract String generateWrapperScript() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    /** This method generates content for a environment variable definition file.
     *  
     * @return the content for a environment variable definition file 
     */
    protected abstract String generateEnvVarFile() throws TapisException;
    
    /* ---------------------------------------------------------------------- */
    /* initBashScript:                                                        */
    /* ---------------------------------------------------------------------- */
    protected void initBashScript()
    {
        _cmd.append("#!/bin/bash\n\n");
        appendDescription();
    }
    
    /* ---------------------------------------------------------------------- */
    /* initBashBatchScript:                                                   */
    /* ---------------------------------------------------------------------- */
    protected void initBashBatchScript()
    {
        _cmd.append("#!/bin/bash\n\n");
        appendBatchDescription();
    }
    
    /* ---------------------------------------------------------------------- */
    /* concatAppArguments:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Assemble the application arguments into a single string and then assign
     * them to the caller.  If there are any arguments, the generated 
     * string always begins with a space character.
     * 
     * @return the app argument string or null if there aren't any.
     */
     protected String concatAppArguments()
    {
         // Get the list of user-specified container arguments.
         var parmSet = _job.getParameterSetModel();
         var opts    = parmSet.getAppArgs();
         if (opts == null || opts.isEmpty()) return null;
         
         // Assemble the application's argument string.
         String args = "";
         for (var opt : opts) args += " " + opt.getArg();
         return args;
    }
    
    /* ---------------------------------------------------------------------- */
    /* isAssigned:                                                            */
    /* ---------------------------------------------------------------------- */
    protected void isAssigned(String runtimeName, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", runtimeName, option);
            throw new JobException(msg);
        }
    }
     
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* appendDescription:                                                     */
    /* ---------------------------------------------------------------------- */
    private void appendDescription()
    {
        _cmd.append("# This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmd.append("# of running a Tapis application.  The order of execution is as follows:\n");
        _cmd.append("#\n");
        _cmd.append("#   1. Standard Tapis and user-supplied environment variables are exported.\n");
        _cmd.append("#   2. The application container is run with container options, environment\n");
        _cmd.append("#      variables and application parameters as supplied in the Tapis job,\n");
        _cmd.append("#      application and system definitions.\n");
        _cmd.append("\n");
    }

    /* ---------------------------------------------------------------------- */
    /* appendBatchDescription:                                                */
    /* ---------------------------------------------------------------------- */
    private void appendBatchDescription()
    {
        _cmd.append("# This script was auto-generated by the Tapis Jobs Service for the purpose\n");
        _cmd.append("# of running a Tapis application.  The order of execution is as follows:\n");
        _cmd.append("#\n");
        _cmd.append("#   1. The batch scheduler options are passed to the scheduler, including any\n");
        _cmd.append("#      user-specified, scheduler-managed environment variables.\n");
        _cmd.append("#   2. The application container is run with container options, environment\n");
        _cmd.append("#      variables and application parameters as supplied in the Tapis job,\n");
        _cmd.append("#      application and system definitions.\n");
        _cmd.append("\n");
    }
}
