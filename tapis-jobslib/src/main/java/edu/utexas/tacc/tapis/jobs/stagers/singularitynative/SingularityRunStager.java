package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public class SingularityRunStager
 extends AbstractSingularityStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunStager.class);

    // Container id file suffix.
    private static final String PID_SUFFIX = ".pid";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singularity run command object.
    private final SingularityRunCmd _singularityCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        super(jobCtx);
        _singularityCmd = configureExecCmd();
    }

    /* ********************************************************************** */
    /*                          Protected Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateWrapperScript() throws TapisException 
    {
        // The generated wrapper script will contain a singularity instance 
        // start command that conforms to this format:
        //
        //  singularity instance start [start options...] <container path> <instance name> [startscript args...]
        String cmdText = _singularityCmd.generateExecCmd(_job);
        
        // Build the command file content.
        initBashScript();
        
        // Add the docker command the the command file.
        _cmd.append(cmdText);
        
        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateEnvVarFile() throws TapisException 
    {
        return _singularityCmd.generateEnvVarFileContent();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureExecCmd:                                                      */
    /* ---------------------------------------------------------------------- */
    private SingularityRunCmd configureExecCmd()
     throws TapisException
    {
        // Create and populate the singularity command.
        var singularityCmd = new SingularityRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Write all the environment variables to file.
        singularityCmd.setEnvFile(makeEnvFilePath());
        
        // Set the image.
        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());
        
        // Set the stdout/stderr redirection file.
        var fm = _jobCtx.getJobFileManager();
        singularityCmd.setRedirectFile(
            fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE));

        // ----------------- User and Tapis Definitions -----------------
        // Set all environment variables.
        setEnvVariables(singularityCmd);
        
        // Set the singularity options.
        setSingularityOptions(singularityCmd);
        
        // Set the application arguments.
        setAppArguments(singularityCmd);
                
        return singularityCmd;
    }

    /* ---------------------------------------------------------------------- */
    /* setEnvVariables:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Both the standard tapis and user-supplied environment variables are
     * assigned here.  The user is prevented at job submission time from 
     * setting any environment variable that starts with the reserved "_tapis" 
     * prefix, so collisions are not possible. 
     * 
     * @param singularityCmd the run command to be updated
     */
    private void setEnvVariables(SingularityRunCmd singularityCmd)
    {
        // Get the list of environment variables.
        var parmSet = _job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return;
        
        // Process each environment variable.
        var singularityEnv = singularityCmd.getEnv();
        for (var kv : envList) singularityEnv.add(Pair.of(kv.getKey(), kv.getValue()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* setAppArguments:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Assemble the application arguments into a single string and then assign
     * them to the singularityCmd.  If there are any arguments, the generated 
     * string always begins with a space character.
     * 
     * @param singularityCmd the command to be updated
     */
    private void setAppArguments(SingularityRunCmd singularityCmd)
    {
         // Assemble the application's argument string.
         String args = concatAppArguments();
         if (args != null) singularityCmd.setAppArguments(args);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setSingularityOptions:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Set the singularity options that we allow the user to modify.
     * 
     * @param singularityCmd the run command to be updated
     */
    private void setSingularityOptions(SingularityRunCmd singularityCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getContainerArgs();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
                throw new JobException(msg);
            }
            
            // Get the option and its value if one is provided.
            String option = null;
            String value  = ""; // default value when none provided
            int groupCount = m.groupCount();
            if (groupCount > 1) option = m.group(1);
            if (groupCount > 2) value  = m.group(2);            
            
            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "singularity", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignCmd(singularityCmd, option, value);
        }
    }
}
