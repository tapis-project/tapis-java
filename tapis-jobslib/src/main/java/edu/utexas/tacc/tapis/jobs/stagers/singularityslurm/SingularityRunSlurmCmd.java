package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;
import edu.utexas.tacc.tapis.shared.exceptions.runtime.TapisRuntimeException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public final class SingularityRunSlurmCmd 
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                              Constants                                 */
    /* ********************************************************************** */
    private static final String DIRECTIVE_PREFIX = "#SBATCH ";
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // SBATCH directive mapping used to ease wrapper script generation.
    private TreeMap<String,String> _directives = new TreeMap<>();
    
    // Slurm sbatch parameters.
    private String  array;                 // comma separated list
    private String  account;               // allocation account name
    private String  acctgFreq;             // account data gathering (<datatype>=<interval>)
    private String  extraNodeInfo;         // node selector
    private String  batch;                 // list of needed node features, subset of constraint list
    private String  bb;                    // burst buffer specification
    private String  bbf;                   // burst buffer file name
    private String  begin;                 // begin datetime (YYYY-MM-DD[THH:MM[:SS]])
    private String  clusterConstraint;     // federated cluster constraint
    private String  clusters;              // comma separated list of cluster names that can run job
    private String  comment;               // job comment, automatically double quoted
    private String  constraint;            // list of needed node features
    private boolean contiguous;            // require contiguous nodes
    private String  coreSpec;              // count of cores per node reserved by job for system operations
    private String  coresPerSocket;        // node restriction
    private String  cpuFreq;               // srun frequency request
    private String  cpusPerGpu;            // number of cpus per gpu
    private String  cpusPerTask;           // number of processors per task
    private String  deadline;              // don't run unless can complete before deadline
    private String  delayBoot;             // minutes to delay rebooting nodes to satisfy job feature
    private String  dependency;            // list of jobs this job depends on
    private String  distribution;          // alternate distribution methods for srun
    private String  error;                 // filename template for stdout and stderr
    private String  exclude;               // explicitly exclude certain nodes
    private String  exclusive;             // prohibit node sharing
    private String  export;                // propagate environment variables to app
    private String  exportFile;            // file of null-separated key/value pairs
    private String  getUserEnv;            // capture the user's login environment settings
    private String  gid;                   // group id under which the job is run
    private String  gpus;                  // total number of gpus
    private String  gpuBind;               // bind tasks to specific GPUs
    private String  gpuFreq;               // specify required gpu frequency
    private String  gpusPerNode;           // specify gpus required per node
    private String  gpusPerSocket;         // specify gpus required per socket
    private String  gpusPerTask;           // specify gpus required per task
    private String  gres;                  // comma delimited list of generic consumable resources
    private String  gresFlags;             // generic resource task binding options
    private String  hint;                  // scheduler hints
    private boolean hold;                  // submit job in a held state, unblock using scontrol
    private boolean ignorePbs;             // ignore all "#PBS" and "#BSUB" options in batch script
    private String  input;                 // connect job's stdin to a file
    private String  jobName;               // name the job
    private String  killOnInvalidDep;      // kill job with invalid dependency (yes|no)
    private String  licenses;              // named licenses needed by job
    private String  mailType;              // events that trigger emails
    private String  mailUser;              // target email address
    private String  mcsLabel;              // used with plugins
    private String  mem;                   // real memory required per node (default units are megabytes)
    private String  memPerCpu;             // minimum memory required per allocated CPU
    private String  memPerGpu;             // minimum memory required per allocated GPU
    private String  memBind;               // specify NUMA task/memory binding with affinity plugin
    private String  minCpus;               // minimum number of logical cpus/processors per node
    private String  network;               // network configuration
    private String  nice;                  // nice value within slurm
    private String  nodeFile;              // file containing a list of nodes
    private String  nodeList;              // request a specific list of hosts
    private String  nodes;                 // minimum number of allocated to job
    private String  noKill;                // don't kill job if a node fails (optionally set to "off")
    private boolean noRequeue;             // never restart or requeue job
    private String  ntasks;                // maximum tasks job will launch
    private String  ntasksPerCore;         // maximum tasks per core
    private String  ntasksPerGpu;          // tasks started per gpu
    private String  ntasksPerNode;         // tasks started per node
    private String  ntasksPerSocket;       // maximum tasks per socket
    private boolean overcommit;            // 1 job per node, or 1 task per cpu
    private boolean oversubscribe;         // allocation can over-subscribe resources with other running jobs
    private String  output;                // connect batch script's stdout/stderr to a file
    private String  openMode;              // open output and error files with append or truncate
    private String  partition;             // the queue name
    private String  power;                 // power plugin options
    private String  priority;              // request job priority
    private String  profile;               // use one or more profiles
    private String  propagate;             // propagate specified configurations to compute nodes
    private String  qos;                   // quality of service
    private boolean reboot;                // reboot nodes
    private boolean requeue;               // allow job to be requeued
    private String  reservation;           // allocate resources for the job from the named reservation
    private String  signal;                // signal job when nearing end time
    private String  socketsPerNode;        // minimum sockets per node
    private String  spreadJob;             // spread job over maximum number of nodes
    private String  switches;              // the maximum count of switches required for job allocation
    private String  time;                  // set total run time limit
    private String  threadSpec;            // count of specialized threads per node reserved by the job for system operations
    private String  threadsPerCore;        // select nodes with at least the specified number of threads per core
    private String  timeMin;               // minimum run time limit for the job
    private String  tmp;                   // minimum amount of temporary disk space per node (default units are megabytes)
    private boolean useMinNodes;           // when multiple ranges given, prefer the smaller counts
    private boolean verbose;               // allow sbatch informational messages
    private String  waitAllNodes;          // wait to begin execution until all nodes are ready for use
    private String  wckey;                 // specify wckey to be used with job
    
    // Slurm options not supported.
    //
    //  --chdir, --help, --parsable, --quiet, --test-only
    //  --uid, --usage, --version, --wait, --wrap, 
    //
    // Slurm options automatically set by Jobs and not directly available to users.
    //
    //  --mem, --nodes (-N), --ntasks (-n), --partition (-p), --time (-t)
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated wrapper script will contain a non-heterogeneous 
        // sbatch command that conforms to this template
        //
        // sbatch [OPTIONS...] tapisjob.sh
    
        // The generated tapisjob.sh script will contain the singularity run 
        // command with its options, the designated image and the application
        // arguments.
        //
        //   singularity run [run options...] <image> [args] 
        
        // Create the command buffer.
        final int capacity = 2048;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Fill in the SBATCH directives.
        buf.append(getSBatchDirectives());

        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() {
        // We pass environment variables to singularity from the command line
        // so that they are enbedded in the wrapper script sent to Slurm. 
        //
        // This method should not be called.
        String msg = MsgUtils.getMsg("JOBS_SCHEDULER_GENERATE_ERROR", "slurm");
        throw new TapisRuntimeException(msg);
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getSBatchDirectives:                                                   */
    /* ---------------------------------------------------------------------- */
    private String getSBatchDirectives()
    {
        // Create a buffer to hold the sbatch directives
        // which must appear in the script before any 
        // executable statements.
        final int capacity = 1024;
        var buf = new StringBuilder(capacity);
        
        // Add the sbatch directives in alphabetic order.
        buf.append("# Slurm directives.\n");
        for (var entry : _directives.entrySet()) {
            buf.append(DIRECTIVE_PREFIX);
            buf.append(entry.getKey());
            if (StringUtils.isNotBlank(entry.getValue())) {
                buf.append(" ");
                buf.append(entry.getValue());
            }
            buf.append("\n");
        }
        buf.append("\n");
        
        // Return the sbatch directives.
        return buf.toString();
    }
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public String getArray() {
        return array;
    }

    public void setArray(String array) {
        this.array = array;
        _directives.put("--array", array);
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
        _directives.put("--account", account);
    }

    public String getAcctgFreq() {
        return acctgFreq;
    }

    public void setAcctgFreq(String acctgFreq) {
        this.acctgFreq = acctgFreq;
        _directives.put("--acctg-freq", acctgFreq);
    }

    public String getExtraNodeInfo() {
        return extraNodeInfo;
    }

    public void setExtraNodeInfo(String extraNodeInfo) {
        this.extraNodeInfo = extraNodeInfo;
        _directives.put("--extra-node-info", extraNodeInfo);
    }

    public String getBatch() {
        return batch;
    }

    public void setBatch(String batch) {
        this.batch = batch;
        _directives.put("--batch", batch);
    }

    public String getBb() {
        return bb;
    }

    public void setBb(String bb) {
        this.bb = bb;
        _directives.put("--bb", bb);
    }

    public String getBbf() {
        return bbf;
    }

    public void setBbf(String bbf) {
        this.bbf = bbf;
        _directives.put("--bbf", bbf);
    }

    public String getBegin() {
        return begin;
    }

    public void setBegin(String begin) {
        this.begin = begin;
        _directives.put("--begin", begin);
    }

    public String getClusterConstraint() {
        return clusterConstraint;
    }

    public void setClusterConstraint(String clusterConstraint) {
        this.clusterConstraint = clusterConstraint;
        _directives.put("--cluster-constraint", clusterConstraint);
    }

    public String getClusters() {
        return clusters;
    }

    public void setClusters(String clusters) {
        this.clusters = clusters;
        _directives.put("--clusters", clusters);
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
        _directives.put("--comment", comment);
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
        _directives.put("--constraint", constraint);
    }

    public boolean isContiguous() {
        return contiguous;
    }

    public void setContiguous(boolean contiguous) {
        this.contiguous = contiguous;
        _directives.put("--contiguous", "");
    }

    public String getCoresPerSocket() {
        return coresPerSocket;
    }

    public void setCoresPerSocket(String coresPerSocket) {
        this.coresPerSocket = coresPerSocket;
        _directives.put("--cores-per-socket", coresPerSocket);
    }

    public String getCpuFreq() {
        return cpuFreq;
    }

    public void setCpuFreq(String cpuFreq) {
        this.cpuFreq = cpuFreq;
        _directives.put("--cpu-freq", cpuFreq);
    }

    public String getCpusPerGpu() {
        return cpusPerGpu;
    }

    public void setCpusPerGpu(String cpusPerGpu) {
        this.cpusPerGpu = cpusPerGpu;
        _directives.put("--cpus-per-gpu", cpusPerGpu);
    }

    public String getCpusPerTask() {
        return cpusPerTask;
    }

    public void setCpusPerTask(String cpusPerTask) {
        this.cpusPerTask = cpusPerTask;
        _directives.put("--cpus-per-task", cpusPerTask);
    }

    public String getDeadline() {
        return deadline;
    }

    public void setDeadline(String deadline) {
        this.deadline = deadline;
        _directives.put("--deadline", deadline);
    }

    public String getDelayBoot() {
        return delayBoot;
    }

    public void setDelayBoot(String delayBoot) {
        this.delayBoot = delayBoot;
        _directives.put("--delay-boot", delayBoot);
    }

    public String getDependency() {
        return dependency;
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
        _directives.put("--dependency", dependency);
    }

    public String getDistribution() {
        return distribution;
    }

    public void setDistribution(String distribution) {
        this.distribution = distribution;
        _directives.put("--distribution", distribution);
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
        _directives.put("--error", error);
    }

    public String getExclude() {
        return exclude;
    }

    public void setExclude(String exclude) {
        this.exclude = exclude;
        _directives.put("--exclude", exclude);
    }

    public String getExclusive() {
        return exclusive;
    }

    public void setExclusive(String exclusive) {
        this.exclusive = exclusive;
        _directives.put("--exclusive", exclusive);
    }

    public String getExport() {
        return export;
    }

    public void setExport(String export) {
        this.export = export;
        _directives.put("--export", export);
    }

    public String getExportFile() {
        return exportFile;
    }

    public void setExportFile(String exportFile) {
        this.exportFile = exportFile;
        _directives.put("--export-file", exportFile);
    }

    public String getGetUserEnv() {
        return getUserEnv;
    }

    public void setGetUserEnv(String getUserEnv) {
        this.getUserEnv = getUserEnv;
        _directives.put("--get-user-env", getUserEnv);
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
        _directives.put("--gid", gid);
    }

    public String getGpus() {
        return gpus;
    }

    public void setGpus(String gpus) {
        this.gpus = gpus;
        _directives.put("--gpus", gpus);
    }

    public String getGpuBind() {
        return gpuBind;
    }

    public void setGpuBind(String gpuBind) {
        this.gpuBind = gpuBind;
        _directives.put("--gpu-bind", gpuBind);
    }

    public String getGpuFreq() {
        return gpuFreq;
    }

    public void setGpuFreq(String gpuFreq) {
        this.gpuFreq = gpuFreq;
        _directives.put("--gpu-freq", gpuFreq);
    }

    public String getGpusPerNode() {
        return gpusPerNode;
    }

    public void setGpusPerNode(String gpusPerNode) {
        this.gpusPerNode = gpusPerNode;
        _directives.put("--gpus-per-node", gpusPerNode);
    }

    public String getGpusPerSocket() {
        return gpusPerSocket;
    }

    public void setGpusPerSocket(String gpusPerSocket) {
        this.gpusPerSocket = gpusPerSocket;
        _directives.put("--gpus-per-socket", gpusPerSocket);
    }

    public String getGpusPerTask() {
        return gpusPerTask;
    }

    public void setGpusPerTask(String gpusPerTask) {
        this.gpusPerTask = gpusPerTask;
        _directives.put("--gpus-per-task", gpusPerTask);
    }

    public String getGres() {
        return gres;
    }

    public void setGres(String gres) {
        this.gres = gres;
        _directives.put("--gres", gres);
    }

    public String getGresFlags() {
        return gresFlags;
    }

    public void setGresFlags(String gresFlags) {
        this.gresFlags = gresFlags;
        _directives.put("--gres-flags", gresFlags);
    }

    public String getHint() {
        return hint;
    }

    public void setHint(String hint) {
        this.hint = hint;
        _directives.put("--hint", hint);
    }

    public boolean isHold() {
        return hold;
    }

    public void setHold(boolean hold) {
        this.hold = hold;
        _directives.put("--hold", "");
    }

    public boolean isIgnorePbs() {
        return ignorePbs;
    }

    public void setIgnorePbs(boolean ignorePbs) {
        this.ignorePbs = ignorePbs;
        _directives.put("--ignore-pbs", "");
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
        _directives.put("--input", input);
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
        _directives.put("--job-name", jobName);
    }

    public String getKillOnInvalidDep() {
        return killOnInvalidDep;
    }

    public void setKillOnInvalidDep(String killOnInvalidDep) {
        this.killOnInvalidDep = killOnInvalidDep;
        _directives.put("--kill-on-invalid-dep", killOnInvalidDep);
    }

    public String getLicenses() {
        return licenses;
    }

    public void setLicenses(String licenses) {
        this.licenses = licenses;
        _directives.put("--licenses", licenses);
    }

    public String getMailType() {
        return mailType;
    }

    public void setMailType(String mailType) {
        this.mailType = mailType;
        _directives.put("--mail-type", mailType);
    }

    public String getMailUser() {
        return mailUser;
    }

    public void setMailUser(String mailUser) {
        this.mailUser = mailUser;
        _directives.put("--mail-user", mailUser);
    }

    public String getMcsLabel() {
        return mcsLabel;
    }

    public void setMcsLabel(String mcsLabel) {
        this.mcsLabel = mcsLabel;
        _directives.put("--mcs-label", mcsLabel);
    }

    public String getMem() {
        return mem;
    }

    public void setMem(String mem) {
        this.mem = mem;
        _directives.put("--mem", mem);
    }

    public String getMemPerCpu() {
        return memPerCpu;
    }

    public void setMemPerCpu(String memPerCpu) {
        this.memPerCpu = memPerCpu;
        _directives.put("--mem-per-cpu", memPerCpu);
    }

    public String getMemPerGpu() {
        return memPerGpu;
    }

    public void setMemPerGpu(String memPerGpu) {
        this.memPerGpu = memPerGpu;
        _directives.put("-mem-per-gpu", memPerGpu);
    }

    public String getMemBind() {
        return memBind;
    }

    public void setMemBind(String memBind) {
        this.memBind = memBind;
        _directives.put("--mem-bind", memBind);
    }

    public String getMinCpus() {
        return minCpus;
    }

    public void setMinCpus(String minCpus) {
        this.minCpus = minCpus;
        _directives.put("--mincpus", minCpus);
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
        _directives.put("--network", network);
    }

    public String getNice() {
        return nice;
    }

    public void setNice(String nice) {
        this.nice = nice;
        _directives.put("--nice", nice);
    }

    public String getNodeFile() {
        return nodeFile;
    }

    public void setNodeFile(String nodefile) {
        this.nodeFile = nodefile;
        _directives.put("--nodefile", nodefile);
    }

    public String getNodeList() {
        return nodeList;
    }

    public void setNodeList(String nodeList) {
        this.nodeList = nodeList;
        _directives.put("--nodelist", nodeList);
    }

    public String getNodes() {
        return nodes;
    }

    public void setNodes(String nodes) {
        this.nodes = nodes;
        _directives.put("--nodes", nodes);
    }

    public String getNoKill() {
        return noKill;
    }

    public void setNoKill(String noKill) {
        this.noKill = noKill;
        _directives.put("--no-kill", noKill);
    }

    public boolean isNoRequeue() {
        return noRequeue;
    }

    public void setNoRequeue(boolean noRequeue) {
        this.noRequeue = noRequeue;
        _directives.put("--no-requeue", "");
    }

    public String getNtasks() {
        return ntasks;
    }

    public void setNtasks(String ntasks) {
        this.ntasks = ntasks;
        _directives.put("--ntasks", ntasks);
    }

    public boolean isOvercommit() {
        return overcommit;
    }

    public void setOvercommit(boolean overcommit) {
        this.overcommit = overcommit;
        _directives.put("--overcommit", "");
    }

    public boolean isOversubscribe() {
        return oversubscribe;
    }

    public void setOversubscribe(boolean oversubscribe) {
        this.oversubscribe = oversubscribe;
        _directives.put("--oversubscribe", "");
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
        _directives.put("--output", output);
    }

    public String getOpenMode() {
        return openMode;
    }

    public void setOpenMode(String openMode) {
        this.openMode = openMode;
        _directives.put("--open-mode", openMode);
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
        _directives.put("--partition", partition);
    }

    public String getPower() {
        return power;
    }

    public void setPower(String power) {
        this.power = power;
        _directives.put("--power", power);
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
        _directives.put("--priority", priority);
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
        _directives.put("--profile", profile);
    }

    public String getPropagate() {
        return propagate;
    }

    public void setPropagate(String propagate) {
        this.propagate = propagate;
        _directives.put("--propagate", propagate);
    }

    public String getQos() {
        return qos;
    }

    public void setQos(String qos) {
        this.qos = qos;
        _directives.put("--qos", qos);
    }

    public boolean isReboot() {
        return reboot;
    }

    public void setReboot(boolean reboot) {
        this.reboot = reboot;
        _directives.put("--reboot", "");
    }

    public boolean isRequeue() {
        return requeue;
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
        _directives.put("--requeue", "");
    }

    public String getReservation() {
        return reservation;
    }

    public void setReservation(String reservation) {
        this.reservation = reservation;
        _directives.put("--reservation", reservation);
    }

    public String getCoreSpec() {
        return coreSpec;
    }

    public void setCoreSpec(String coreSpec) {
        this.coreSpec = coreSpec;
        _directives.put("--core-spec", coreSpec);
    }

    public String getSignal() {
        return signal;
    }

    public void setSignal(String signal) {
        this.signal = signal;
        _directives.put("--signal", signal);
    }

    public String getSocketsPerNode() {
        return socketsPerNode;
    }

    public void setSocketsPerNode(String socketsPerNode) {
        this.socketsPerNode = socketsPerNode;
        _directives.put("--sockets-per-node", socketsPerNode);
    }

    public String getSpreadJob() {
        return spreadJob;
    }

    public void setSpreadJob(String spreadJob) {
        this.spreadJob = spreadJob;
        _directives.put("--spread-job", spreadJob);
    }

    public String getSwitches() {
        return switches;
    }

    public void setSwitches(String switches) {
        this.switches = switches;
        _directives.put("--switches", switches);
    }

    public String getNTasksPerCore() {
        return ntasksPerCore;
    }

    public void setNTasksPerCore(String ntasksPerCore) {
        this.ntasksPerCore = ntasksPerCore;
        _directives.put("--ntasks-per-core", ntasksPerCore);
    }

    public String getNTasksPerGpu() {
        return ntasksPerGpu;
    }

    public void setNTasksPerGpu(String ntasksPerGpu) {
        this.ntasksPerGpu = ntasksPerGpu;
        _directives.put("--ntasks-per-gpu", ntasksPerGpu);
    }

    public String getNTasksPerNode() {
        return ntasksPerNode;
    }

    public void setNTasksPerNode(String ntasksPerNode) {
        this.ntasksPerNode = ntasksPerNode;
        _directives.put("--ntasks-per-node", ntasksPerNode);
    }

    public String getNTasksPerSocket() {
        return ntasksPerSocket;
    }

    public void setNTasksPerSocket(String ntasksPerSocket) {
        this.ntasksPerSocket = ntasksPerSocket;
        _directives.put("--ntasks-per-socket", ntasksPerSocket);
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
        _directives.put("--time", time);
    }

    public String getThreadSpec() {
        return threadSpec;
    }

    public void setThreadSpec(String threadSpec) {
        this.threadSpec = threadSpec;
        _directives.put("--thread-spec", threadSpec);
    }

    public String getThreadsPerCore() {
        return threadsPerCore;
    }

    public void setThreadsPerCore(String threadsPerCore) {
        this.threadsPerCore = threadsPerCore;
        _directives.put("--threads-per-core", threadsPerCore);
    }

    public String getTimeMin() {
        return timeMin;
    }

    public void setTimeMin(String timeMin) {
        this.timeMin = timeMin;
        _directives.put("--time-min", timeMin);
    }

    public String getTmp() {
        return tmp;
    }

    public void setTmp(String tmp) {
        this.tmp = tmp;
        _directives.put("--tmp", tmp);
    }

    public boolean isUseMinNodes() {
        return useMinNodes;
    }

    public void setUseMinNodes(boolean useMinNodes) {
        this.useMinNodes = useMinNodes;
        _directives.put("--use-min-nodes", "");
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
        _directives.put("--verbose", "");
    }

    public String getWaitAllNodes() {
        return waitAllNodes;
    }

    public void setWaitAllNodes(String waitAllNodes) {
        this.waitAllNodes = waitAllNodes;
        _directives.put("--wait-all-nodes", waitAllNodes);
    }

    public String getWckey() {
        return wckey;
    }

    public void setWckey(String wckey) {
        this.wckey = wckey;
        _directives.put("--wckey", wckey);
    }
}
