package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SingularityStartStager
 extends AbstractSingularityStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityStartStager.class);

    // Container id file suffix.
    private static final String PID_SUFFIX = ".pid";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Singularity run command object.
    private final SingularityStartCmd _singularityCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityStartStager(JobExecutionContext jobCtx)
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
    private SingularityStartCmd configureExecCmd()
     throws TapisException
    {
        // Create and populate the singularity command.
        var singularityCmd = new SingularityStartCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Container instances are named after the job uuid.
        singularityCmd.setName(_job.getUuid());
        
        // Write the container id to a host file.
        setPidFile(singularityCmd);
        
        // Write all the environment variables to file.
        singularityCmd.setEnvFile(makeEnvFilePath());
        
        // Set the image.
        singularityCmd.setImage(_jobCtx.getApp().getContainerImage());

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
    /* setPidFile:                                                            */
    /* ---------------------------------------------------------------------- */
    /** Write the process id associated with the job instance to a fhost file.
     * 
     * @param singularityCmd the run command to be updated
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    private void setPidFile(SingularityStartCmd singularityCmd) 
     throws TapisException
    {
        // Put the cid file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        String path = fm.makeAbsExecSysExecPath(_job.getUuid() + PID_SUFFIX);
        singularityCmd.setPidFile(path);
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
    private void setEnvVariables(SingularityStartCmd singularityCmd)
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
    private void setAppArguments(SingularityStartCmd singularityCmd)
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
    private void setSingularityOptions(SingularityStartCmd singularityCmd)
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
            if (groupCount > 0) option = m.group(1);
            if (groupCount > 1) value  = m.group(2);            
            
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
