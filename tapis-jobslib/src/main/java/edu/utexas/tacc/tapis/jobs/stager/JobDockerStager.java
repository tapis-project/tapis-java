package edu.utexas.tacc.tapis.jobs.stager;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd;
import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd.AttachEnum;
import edu.utexas.tacc.tapis.jobs.stager.runtimes.DockerRunCmd.BindMount;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

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
    // the option and value.  The optional equals sign is also ignored, whether
    // there's whitespace on either side of it or not.
    // (\s=whitespace, \S=not whitespace)
    private static final Pattern _optionPattern = Pattern.compile("\s*(--?[^=\s]*)\s*=?\s*(\\S*)\s*");
    
    // Split port values that can have the maximal form: ipaddr:port:port/protocol,
    // such as 127.0.0.1:80:8080/tcp.  NOTE: We currently only support ipv4 port 
    // mappings.
    private static final Pattern _portPattern = Pattern.compile("[:/]");

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
     throws TapisException
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
     throws JobException
    {
        // Create and populate the docker command.
        var dockerRunCmd = new DockerRunCmd();
        
        // ----------------- Tapis Standard Definitions -----------------
        // Containers are named after the job uuid.
        dockerRunCmd.setName(_job.getUuid());
        
        // Remove the container after it runs.
        dockerRunCmd.setRm(true);
        
        // Set the standard bind mounts.
        setStandardBindMounts(dockerRunCmd);
        
        // Set all environment variables.
        setEnvVariables(dockerRunCmd);
        
        // Set the docker options.
        setDockerOptions(dockerRunCmd);
        
        return dockerRunCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setStandardBindMounts:                                                 */
    /* ---------------------------------------------------------------------- */
    /** Tapis mounts the execution system's input, output and exec directories
     * using the same, standard Tapis mountpoints in each container.
     * 
     * @param dockerRunCmd the run command to be updated
     */
    private void setStandardBindMounts(DockerRunCmd dockerRunCmd)
    {
        // Set standard bind mounts.
        var mount = new BindMount();
        mount.setSource(_job.getExecSystemInputDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_INPUT_MOUNTPOINT);
        mount.setReadOnly(true);
        dockerRunCmd.getMount().add(mount.toString());
        
        mount = new BindMount();
        mount.setSource(_job.getExecSystemOutputDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_OUTPUT_MOUNTPOINT);
        dockerRunCmd.getMount().add(mount.toString());
        
        mount = new BindMount();
        mount.setSource(_job.getExecSystemExecDir());
        mount.setTarget(Job.DEFAULT_EXEC_SYSTEM_EXEC_MOUNTPOINT);
        dockerRunCmd.getMount().add(mount.toString());
    }
    
    /* ---------------------------------------------------------------------- */
    /* setEnvVariables:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Both the standard tapis and user-supplied environment variables are
     * assigned here.  The user is prevented at job submission time from 
     * setting any environment variable that starts with the reserved "_tapis" 
     * prefix, so collisions are not possible. 
     * 
     * @param dockerRunCmd the run command to be updated
     */
    private void setEnvVariables(DockerRunCmd dockerRunCmd)
    {
        // Get the list of environment variable.
        var parmSet = _job.getParameterSetModel();
        var envList = parmSet.getEnvVariables();
        if (envList == null || envList.isEmpty()) return;
        
        // Process each environment variable.
        var dockerEnv = dockerRunCmd.getEnv();
        for (var kv : envList) dockerEnv.add(Pair.of(kv.getKey(), kv.getValue()));
    }
    
    /* ---------------------------------------------------------------------- */
    /* setDockerOptions:                                                      */
    /* ---------------------------------------------------------------------- */
    /** Set the docker options that we allow the user to modify.
     * 
     * @param dockerRunCmd the run command to be updated
     */
    private void setDockerOptions(DockerRunCmd dockerRunCmd)
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
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "docker", opt.getArg());
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
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_ARG_PARSE_ERROR", "docker", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignRunCmd(dockerRunCmd, option, value);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* assignRunCmd:                                                          */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified docker parameter.
     * 
     * @param dockerRunCmd the run command
     * @param option the docker argument
     * @param value the argument's non-null value
     */
    private void assignRunCmd(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            case "--add-host":
                dockerRunCmd.setAddHost(value); // Should be name:ipaddr format
                break;
            case "--attach":
            case "-a":
                attachIO(dockerRunCmd, option, value);
                break;
            case "--cidfile":
                dockerRunCmd.setCidFile(value); // File name can reference env vars
                break;
            case "--cpus":
                dockerRunCmd.setCpus(value); // Value will be doublequoted
                break;
            case "--cpuset-cpus":
                dockerRunCmd.setCpusetCPUs(value);
                break;
            case "--cpuset-mems":
                dockerRunCmd.setCpusetMEMs(value);
                break;
            case "--env":
            case "-e":
                // Already set, so ignore.    
                break;
            case "--gpus":
                dockerRunCmd.setGpus(value);
                break;
            case "--group-add":
                addGroup(dockerRunCmd, option, value);
                break;
            case "--hostname":
            case "-h":
                dockerRunCmd.setHostName(value);
                break;
            case "--ip":
                dockerRunCmd.setIp(value);
                break;
            case "--ip6":
                dockerRunCmd.setIp6(value);
                break;
            case "--label":
                addLabel(dockerRunCmd, option, value);
            case "-l":
                break;
            case "--log-driver":
                dockerRunCmd.setLogDriver(value);
                break;
            case "--log-opt":
                dockerRunCmd.setLogOpts(value);
                break;
            case "--memory":
            case "-m":
                dockerRunCmd.setMemory(value);
                break;
            case "--mount":
                addMount(dockerRunCmd, option, value);
                break;
            case "--network":
            case "--net":
                dockerRunCmd.setNetwork(value);
                break;
            case "--network-alias":
            case "--net-alias":
                dockerRunCmd.setNetworkAlias(value);
                break;
            case "--publish":
            case "-p":
                addPort(dockerRunCmd, option, value);
                break;
            case "--rm":
                // Always set, ignore redundancy.
                break;
            case "--tmpfs":
                addTmpfs(dockerRunCmd, option, value);
                break;
            case "--volume":
            case "-v":
                addVolumeMount(dockerRunCmd, option, value);
                break;
            case "--workdir":
                dockerRunCmd.setWorkdir(value);
                break;
                
            default:
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_UNSUPPORTED_ARG", "docker", option);
                throw new JobException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* attachIO:                                                              */
    /* ---------------------------------------------------------------------- */
    private void attachIO(DockerRunCmd dockerRunCmd, String option, String value) 
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // Make sure the value is valid.
        AttachEnum v;
        try {v = AttachEnum.valueOf(value.toLowerCase());}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_INVALID_ARG", "docker", option, value);
                throw new JobException(msg, e);
            }
        
        // Add value to attach list.
        dockerRunCmd.getAttachList().add(v);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addGroup:                                                              */
    /* ---------------------------------------------------------------------- */
    private void addGroup(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // This is a repeatable option where each occurrence specifies a single group.
        dockerRunCmd.getGroups().add(value);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addLabel:                                                              */
    /* ---------------------------------------------------------------------- */
    private void addLabel(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // Find the first equals sign.  We expect the value to be in key=text format.
        int index = value.indexOf("=");
        if (index < 1) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_INVALID_ARG", "docker", option, value);
            throw new JobException(msg);
        }
        String key  = value.substring(0, index);
        String text = value.substring(index+1);
        
        // This is a repeatable option where each occurrence specifies a single group.
        dockerRunCmd.getLabels().add(Pair.of(key, text));
    }
    
    /* ---------------------------------------------------------------------- */
    /* addPort:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Add a ipv4 port mapping between host and container. */ 
    private void addPort(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        // Add the port to the port list.
        dockerRunCmd.getPortMappings().add(value);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addMount:                                                              */
    /* ---------------------------------------------------------------------- */
    private void addMount(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // Add the value to the mount list.
        dockerRunCmd.getMount().add(value);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addTmpfs:                                                              */
    /* ---------------------------------------------------------------------- */
    private void addTmpfs(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // Add the value to the tmpfs list.
        dockerRunCmd.getTmpfs().add(value);
    }
    
    /* ---------------------------------------------------------------------- */
    /* addVolumeMount:                                                        */
    /* ---------------------------------------------------------------------- */
    private void addVolumeMount(DockerRunCmd dockerRunCmd, String option, String value)
     throws JobException
    {
        // Make sure we have a value.
        if (StringUtils.isBlank(value)) {
            String msg = MsgUtils.getMsg("JOBS_CONTAINER_MISSING_ARG_VALUE", "docker", option);
            throw new JobException(msg);
        }
        
        // Add the value to the volume list.
        dockerRunCmd.getVolumeMount().add(value);
    }
}