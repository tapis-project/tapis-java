package edu.utexas.tacc.tapis.jobs.stager;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd;
import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd.BindMount;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;

public final class JobDockerStager 
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(JobDockerStager.class);
    
    // Docker command line option parser.  This regex captures 3 groups:
    //
    //   0 - the complete value unparsed
    //   1 - the docker option starting with 1 or 2 hypens (-e, --env, etc.)
    //   2 - the value assigned to the option, which may be empty
    //
    // Leading and trailing whitespace is ignored, as is any whitespace between
    // the option and value.  The optional equals sign is also ignored. 
    // (\s=whitespace, \S=not whitespace)
    private static final Pattern _optionPattern = Pattern.compile("\s*(--?\\S*)\s*=?\s*(\\S*)\s*");

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public JobDockerStager(JobExecutionContext jobCtx){super(jobCtx);}

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateWrapperScript() 
    {
        // Create and populate the docker command.
        var dockerRun = configureRunCmd();
        
        // Build the command file content.
        initBashScript();
        
        return _cmd.toString();
    }
    
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureRunCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    private DockerRunCmd configureRunCmd()
    {
        // Create and populate the docker command.
        var dockerRun = new DockerRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Containers are named after the job uuid.
        dockerRun.setName(_job.getUuid());
        
        // Remove the container after it runs.
        dockerRun.setRm(true);
        
        // Set the standard bind mounts.
        setStandardBindMounts(dockerRun);
        
        // Set all environment variables.
        setEnvVariables(dockerRun);
        
        // Set the docker options.
        setDockerOptions(dockerRun);
        
        return dockerRun;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setStandardBindMounts:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Tapis mounts the execution system's input, output and exec directories
     * using the same, standard Tapis mountpoints in each container.
     * 
     * @param dockerRun the run command to be updated
     */
    private void setStandardBindMounts(DockerRunCmd dockerRun)
    {
        // Set standard bind mounts.
        var mount = new BindMount();
        mount.setSource(_job.getExecSystemInputDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_INPUT_MOUNTPOINT);
        mount.setReadOnly(true);
        dockerRun.getBindMount().add(mount);
        
        mount = new BindMount();
        mount.setSource(_job.getExecSystemOutputDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_OUTPUT_MOUNTPOINT);
        dockerRun.getBindMount().add(mount);
        
        mount = new BindMount();
        mount.setSource(_job.getExecSystemExecDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_EXEC_MOUNTPOINT);
        dockerRun.getBindMount().add(mount);
    }
    
    /* ---------------------------------------------------------------------- */
    /* setEnvVariables:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Both the standard tapis and user-supplied environment variables are
     * assigned here.  The user is prevented at job submission time from 
     * setting any environment variable that starts with the reserved "_tapis" 
     * prefix, so collisions are not possible. 
     * 
     * @param dockerRun the run command to be updated
     */
    private void setEnvVariables(DockerRunCmd dockerRun)
    {
        // Get the list of environment variable.
        var parmSet = _job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return;
        
        // Process each environment variable.
        var dockerEnv = dockerRun.getEnv();
        for (var kv : envList) dockerEnv.add(Pair.of(kv.getKey(), kv.getValue()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* setDockerOptions:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Set the docker options that we allow the user to modify.
     * 
     * @param dockerRun the run command to be updated
     */
    private void setDockerOptions(DockerRunCmd dockerRun)
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
                
            }
            
            // Get the option and its value if one is provided.
            String option = null;
            String value  = null;
            int groupCount = m.groupCount();
            if (groupCount > 1) option = m.group(1);
            if (groupCount > 2) value  = m.group(2);
            
            // The option should always exist.
            if (StringUtils.isBlank(option)) {
                
            }
            
            

        }
    }
}