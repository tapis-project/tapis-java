package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.singularitynative.AbstractSingularityStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobFileManager;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SingularityRunSlurmStager 
  extends AbstractSingularityStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmStager.class);

    // Temporary hardcoded TACC module load command.
    private static final String TACC_SINGULARITY_MODULE_LOAD = "module load tacc-singularity";
    
    // Create the pattern for a list of one or more comma or semi-colon separated 
    // names with no embedded whitespace.
    private static final Pattern _namePattern = Pattern.compile("[,;\\s]");
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Slurm run command object.
    private final SingularityRunSlurmCmd _slurmRunCmd;
    
    // Embedded singularity stager.
    private final WrappedSingularityRunStager _wrappedStager;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public SingularityRunSlurmStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        // The singularity stager must initialize before the slurm run command.
        super(jobCtx);
        _wrappedStager = new WrappedSingularityRunStager(jobCtx);
        _slurmRunCmd   = configureSlurmRunCmd();
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
        
        // Install the wrapper script on the execution system.
        var fm = _jobCtx.getJobFileManager();
        fm.installExecFile(wrapperScript, JobExecutionUtils.JOB_WRAPPER_SCRIPT, JobFileManager.RWXRWX);
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
        // Initialize the script content in superclass.
        initBashBatchScript();
        
        // Add the batch directives to the script.
        _cmd.append(_slurmRunCmd.generateExecCmd(_job));
        
        // Add zero or more module load commands.
        _cmd.append(getModuleLoadCalls(TACC_SINGULARITY_MODULE_LOAD));
        
        // Add the actual singularity run command.
        _cmd.append(_wrappedStager.getCmdTextWithEnvVars());
        _cmd.append("/n");
        
        return _cmd.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateEnvVarFile() throws TapisException 
    {
        String msg = MsgUtils.getMsg("JOBS_SINGULARITY_INVALID_ENV_CALL", 
                                     _job.getUuid(), "generateEnvVarFile");
        throw new TapisException(msg);
    }
    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getModuleLoadCalls:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Create a list of module load calls, each on it own line, from the 
     * input string.  The input can be a comma or semicolon separated list,
     * whitespace is allowed between calls. 
     * 
     * @param calls string containing zero or more comma or semicolon separated calls
     * @return the list of calls on separate lines
     */
    private String getModuleLoadCalls(String calls)
    {
        // The result string.
        if (StringUtils.isBlank(calls)) return "";
        String result = "";
        
        // Split the calls into separate lines.
        String[] lines = _namePattern.split(calls);
        if (lines == null || lines.length == 0) return result;
        for (var line : lines) 
            if (StringUtils.isNotBlank(line)) result += line + "\n"; 
        result += "\n";
        
        // Each call on its own line.
        return result;
    }
    
    /* ---------------------------------------------------------------------- */
    /* configureSlurmRunCmd:                                                  */
    /* ---------------------------------------------------------------------- */
    /** Initialize a new slurm run command with user and tapis specified options.
     * The signularity stager field must be initialized before calling this
     * method.
     * 
     * @return the initialized slurm run command
     * @throws TapisException
     */
    private SingularityRunSlurmCmd configureSlurmRunCmd()
     throws TapisException
    {
        // Populate the slurm command with user specified options.
        var slurmCmd = new SingularityRunSlurmCmd();
        setUserSlurmOptions(slurmCmd);
        setTapisOptionsForSlurm(slurmCmd);
        return slurmCmd;
    }
    
    /* ---------------------------------------------------------------------- */
    /* setTapisOptionsForSlurm:                                               */
    /* ---------------------------------------------------------------------- */
    /** Set the standard Tapis settings for Slurm.
     * 
     * TODO: At this time we always omit setting the --mem option as required by TACC systems.
     * 
     * @param slurmCmd
     * @throws TapisException 
     */
    private void  setTapisOptionsForSlurm(SingularityRunSlurmCmd slurmCmd) 
     throws TapisException
    {
        // --------------------- Tapis Mandatory ---------------------
        // Request the total number of nodes from slurm. 
        slurmCmd.setNodes(Integer.toString(_job.getNodeCount()));
        
        // Tell slurm the total number of tasks to run.
        slurmCmd.setNtasks(Integer.toString(_job.getTotalTasks()));
        
        // Tell slurm the total runtime of the application in minutes.
        slurmCmd.setTime(Integer.toString(_job.getMaxMinutes()));
        
        // Tell slurm the memory per node requirement in megabytes.
        slurmCmd.setMem(Integer.toString(_job.getMemoryMB()));
        
        // We've already checked in JobQueueProcessor before processing any
        // state changes that the logical and hpc queues have been assigned.
        var logicalQueue = _jobCtx.getLogicalQueue();
        slurmCmd.setPartition(logicalQueue.getHpcQueueName());
        
        // --------------------- Tapis Optional ----------------------
        // Always assign a job name if user has not specified one.
        if (StringUtils.isBlank(slurmCmd.getJobName())) {
            var singularityRunCmd = _wrappedStager.getSingularityRunCmd();
            String image = singularityRunCmd.getImage();
            var parts = image.split("/");
            
            // The last part element should be present and never empty.
            if (parts == null || parts.length == 0) 
                slurmCmd.setJobName(JobExecutionUtils.JOB_WRAPPER_SCRIPT);
              else slurmCmd.setJobName(parts[parts.length-1]);
        }
        
        // Assign the standard tapis output file name if one is not 
        // assigned and we are not running an array job.  We let slurm
        // use its default naming scheme for array job output files.
        // Unless the user explicitly specifies an error file, both
        // stdout and stderr will go the designated output file.
        if (StringUtils.isBlank(slurmCmd.getOutput()) && 
            StringUtils.isBlank(slurmCmd.getArray())) {
            var fm = _jobCtx.getJobFileManager();
            slurmCmd.setOutput(
                fm.makeAbsExecSysOutputPath(JobExecutionUtils.JOB_OUTPUT_REDIRECT_FILE));
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* setUserSlurmOptions:                                                   */
    /* ---------------------------------------------------------------------- */
    /** Set the slurm options that we allow the user to modify.
     * 
     * @param slurmCmd the run command to be updated
     */
    private void setUserSlurmOptions(SingularityRunSlurmCmd slurmCmd)
     throws JobException
    {
        // Get the list of user-specified container arguments.
        var parmSet = _job.getParameterSetModel();
        var opts    = parmSet.getSchedulerOptions();
        if (opts == null || opts.isEmpty()) return;
        
        // Iterate through the list of options.
        for (var opt : opts) {
            var m = _optionPattern.matcher(opt.getArg());
            boolean matches = m.matches();
            if (!matches) {
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
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
                String msg = MsgUtils.getMsg("JOBS_SCHEDULER_ARG_PARSE_ERROR", "slurm", opt.getArg());
                throw new JobException(msg);
            }
            
            // Save the parsed value.
            assignCmd(slurmCmd, option, value);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity parameter.
     * 
     * @param slurmCmd the start command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected void assignCmd(SingularityRunSlurmCmd slurmCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            // Start/Run common options.
            case "--array":
            case "-a":
                slurmCmd.setArray(value);
                break;
                
            case "--account":
            case "-A":
                slurmCmd.setAccount(value);
                break;
                
            case "--acctg-freq":
                slurmCmd.setAcctgFreq(value);
                break;
                
            case "--extra-node-info":
            case "-B":
                slurmCmd.setExtraNodeInfo(value);
                break;
                
            case "--batch":
                slurmCmd.setBatch(value);
                break;
                
            case "--bb":
                slurmCmd.setBb(value);
                break;
                
            case "--bbf":
                slurmCmd.setBbf(value);
                break;
                
            case "--begin":
            case "-b":
                slurmCmd.setArray(value);
                break;
                
            case "--cluster-contstraint":
                slurmCmd.setClusterConstraint(value);
                break;
                
            case "--clusters":
            case "-M":
                slurmCmd.setClusters(value);
                break;
                
            case "--comment":
                slurmCmd.setComment(value);
                break;
                
            case "--constraint":
            case "-C":
                slurmCmd.setConstraint(value);
                break;
                
            case "--contiguous":
                slurmCmd.setContiguous(true);
                break;
                
            case "--core-spec":
            case "-S":
                slurmCmd.setCoreSpec(value);
                break;
                
            case "--cores-per-socket":
                slurmCmd.setCoresPerSocket(value);
                break;
                
            case "--cpu-freq":
                slurmCmd.setCpuFreq(value);
                break;
                
            case "--cpus-per-gpu":
                slurmCmd.setCpusPerGpu(value);
                break;
                
            case "--cpus-per-task":
                slurmCmd.setCpusPerTask(value);
                break;
                
            case "--deadline":
                slurmCmd.setDeadline(value);
                break;
                
            case "--delay-boot":
                slurmCmd.setDelayBoot(value);
                break;
                
            case "--dependency":
            case "-d":
                slurmCmd.setDependency(value);
                break;
                
            case "--distribution":
            case "-m":
                slurmCmd.setDistribution(value);
                break;
                
            case "--error":
            case "-e":
                slurmCmd.setError(value);
                break;
                
            case "--exclude":
            case "-X":
                slurmCmd.setExclusive(value);
                break;
                
            case "--exclusive":
                slurmCmd.setExclusive(value);
                break;
                
            case "--export":
                slurmCmd.setExport(value);
                break;
                
            case "--export-file":
                slurmCmd.setExportFile(value);
                break;
                
            case "--get-user-env":
                slurmCmd.setGetUserEnv(value);
                break;
                
            case "--gid":
                slurmCmd.setGid(value);
                break;
                
            case "--gpus":
            case "-G":
                slurmCmd.setArray(value);
                break;
                
            case "--gpu-bind":
                slurmCmd.setGpuBind(value);
                break;
                
            case "--gpu-freq":
                slurmCmd.setGpuFreq(value);
                break;
                
            case "--gpus-per-node":
                slurmCmd.setGpusPerNode(value);
                break;
                
            case "--gpus-per-socket":
                slurmCmd.setGpusPerSocket(value);
                break;
                
            case "--gpus-per-task":
                slurmCmd.setGpusPerTask(value);
                break;
                
            case "--gres":
                slurmCmd.setGres(value);
                break;
                
            case "--gres-flags":
                slurmCmd.setGresFlags(value);
                break;
                
            case "--hint":
                slurmCmd.setHint(value);
                break;
                
            case "--hold":
            case "-H":
                slurmCmd.setHold(true);
                break;
                
            case "--ignore-pbs":
                slurmCmd.setIgnorePbs(true);
                break;
                
            case "--input":
            case "-i":
                slurmCmd.setInput(value);
                break;
                
            case "--job-name":
            case "-J":
                slurmCmd.setJobName(value);
                break;
                
            case "--kill-on-invalid-dep":
                slurmCmd.setKillOnInvalidDep(value);
                break;
                
            case "--licenses":
            case "-L":
                slurmCmd.setLicenses(value);
                break;
                
            case "--mail-type":
                slurmCmd.setMailType(value);
                break;
                
            case "--mail-user":
                slurmCmd.setMailUser(value);
                break;
                
            case "--mcs-label":
                slurmCmd.setMcsLabel(value);
                break;
                
            case "--mem-per-cpu":
                slurmCmd.setMemPerCpu(value);
                break;
                
            case "--mem-per-gpu":
                slurmCmd.setMemPerGpu(value);
                break;
                
            case "--mem-bind":
                slurmCmd.setMemBind(value);
                break;
                
            case "--mincpus":
                slurmCmd.setMinCpus(value);
                break;
                
            case "--network":
                slurmCmd.setNetwork(value);
                break;
                
            case "--nice":
                slurmCmd.setNice(value);
                break;
                
            case "--nodefile":
            case "-F":
                slurmCmd.setNodeFile(value);
                break;
                
            case "--nodelist":
            case "-W":
                slurmCmd.setArray(value);
                break;
                
            case "--no-kill":
            case "-k":
                slurmCmd.setNoKill(value);
                break;
                
            case "--no-requeue":
                slurmCmd.setNoRequeue(true);
                break;
                
            case "--ntasks-per-core":
                slurmCmd.setNTasksPerCore(value);
                break;
                
            case "--ntasks-per-gpu":
                slurmCmd.setNTasksPerGpu(value);
                break;
                
            case "--ntasks-per-node":
                slurmCmd.setNTasksPerNode(value);
                break;
                
            case "--ntasks-per-socket":
                slurmCmd.setNTasksPerSocket(value);
                break;
                
            case "--overcommit":
            case "-O":
                slurmCmd.setOvercommit(true);
                break;
                
            case "--oversubscribe":
            case "-s":
                slurmCmd.setOversubscribe(true);
                break;
                
            case "--output":
            case "-o":
                slurmCmd.setOutput(value);
                break;
                
            case "--open-mode":
                slurmCmd.setOpenMode(value);
                break;
                
            case "--power":
                slurmCmd.setPower(value);
                break;
                
            case "--priority":
                slurmCmd.setPriority(value);
                break;
                
            case "--profile":
                slurmCmd.setProfile(value);
                break;
                
            case "--propagate":
                slurmCmd.setPropagate(value);
                break;
                
            case "--qos":
            case "-q":
                slurmCmd.setQos(value);
                break;
                
            case "--reboot":
                slurmCmd.setReboot(true);
                break;
                
            case "--requeue":
                slurmCmd.setRequeue(true);
                break;

            case "--reservation":
                slurmCmd.setReservation(value);
                break;
                
            case "--signal":
                slurmCmd.setSignal(value);
                break;
                
            case "--sockets-per-node":
                slurmCmd.setSocketsPerNode(value);
                break;
                
            case "--spread-job":
                slurmCmd.setSpreadJob(value);
                break;
                
            case "--switches":
                slurmCmd.setSwitches(value);
                break;
                
            case "--thread-spec":
                slurmCmd.setThreadSpec(value);
                break;
                
            case "--threads-per-core":
                slurmCmd.setThreadsPerCore(value);
                break;
                
            case "--time-min":
                slurmCmd.setTimeMin(value);
                break;
                
            case "--tmp":
                slurmCmd.setTmp(value);
                break;
                
            case "--use-min-nodes":
                slurmCmd.setUseMinNodes(true);
                break;
                
            case "--verbose":
            case "-v":
                // Multiple -v's increase verbosity, 
                // we only support the first level. 
                slurmCmd.setVerbose(true);
                break;
                
            case "--wait-all-nodes":
                slurmCmd.setWaitAllNodes(value);
                break;
                
            case "--wckey":
                slurmCmd.setWckey(value);
                break;

            
            // Subsumed options.
            case "--mem":
            case "--nodes":
            case "-N":
            case "--ntasks":
            case "-n":
            case "--partition":
            case "-p":
            case "--time":
            case "-t":
                String tapisArg = getTapisArg(option);
                String msg1 = MsgUtils.getMsg("JOBS_SCHEDULER_SUBSUMED_ARG", "slurm", option, tapisArg);
                throw new JobException(msg1);
                
                
            default:
                // Slurm options not supported:
                //
                //  --chdir, --help, --parsable, --quiet, --test-only
                //  --uid, --usage, --version, --wait, --wrap, 
                //
                // Slurm options automatically set by Jobs and not directly available to users.
                // These are the above subsumed options.
                //
                //  --mem, --nodes (-N), --ntasks (-n), --partition (-p), --time (-t)
                //
                String msg2 = MsgUtils.getMsg("JOBS_SCHEDULER_UNSUPPORTED_ARG", "slurm", option);
                throw new JobException(msg2);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    private String getTapisArg(String option)
    {
        return switch (option) {
            case "--mem"       -> "memoryMB";
            case "--nodes"     -> "nodeCount";
            case "-N"          -> "nodeCount";
            case "--ntasks"    -> "coresPerNode";
            case "-n"          -> "coresPerNode";
            case "--partition" -> "execSystemLogicalQueue";
            case "-p"          -> "execSystemLogicalQueue";
            case "--time"      -> "maxMinutes";
            case "-t"          -> "maxMinutes";
            default            -> "unknown";
        };
    }
}
