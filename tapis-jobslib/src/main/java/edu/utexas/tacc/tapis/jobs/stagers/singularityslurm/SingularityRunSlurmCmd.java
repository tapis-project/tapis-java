package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;

public final class SingularityRunSlurmCmd 
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Slurm parameters are in short form order (ex: --exclude ordered using -x)
    private String array;                 // comma separated list
    private String account;               // allocation account name
    private String acctgFreq;             // account data gathering (<datatype>=<interval>)
    private String extraNodeInfo;         // node selector
    private String batch;                 // list of needed node features, subset of constraint list
    private String bb;                    // burst buffer specification
    private String bbf;                   // burst buffer file name
    private String begin;                 // begin datetime (YYYY-MM-DD[THH:MM[:SS]])
    private String clusterConstraint;     // federated cluster constraint
    private String comment;               // job comment, automatically double quoted
    private String constraint;            // list of needed node features
    private String contiguous;            // require contiguous nodes
    private String coresPerSocket;        // node restriction
    private String cpuFreq;               // srun frequency request
    private String cpusPerGpu;            // number of cpus per gpu
    private String cpusPerTask;           // number of processors per task
    private String deadline;              // don't run unless can complete before deadline
    private String delayBoot;             // minutes to delay rebooting nodes to satisfy job feature
    private String dependency;            // list of jobs this job depends on
    private String error;                 // filename template for stdout and stderr
    private String exclusive;             // prohibit node sharing
    private String export;                // propagate environment variables to app
    private String exportFile;            // file of null-separated key/value pairs
    private String nodefile;              // file containing a list of nodes
    private String getUserEnv;            // capture the user's login environment settings
    private String gid;                   // group id under which the job is run
    private String gpus;                  // total number of gpus
    private String gpuBind;               // bind tasks to specific GPUs
    private String gpuFreq;               // specify required gpu frequency
    private String gpusPerNode;           // specify gpus required per node
    private String gpusPerSocket;         // specify gpus required per socket
    private String gpusPerTask;           // specify gpus required per task
    private String gres;                  // comma delimited list of generic consumable resources
    private String gresFlags;             // generic resource task binding options
    private String hint;                  // scheduler hints
    private String input;                 // connect job's stdin to a file
    private String jobName;               // name the job
    private boolean noKill;               // don't kill job if a node fails
    private boolean killOnInvalidDep;     // kill job with invald dependency
    private String licenses;              // named licenses needed by job
    private String clusters;              // comma separated list of cluster names that can run job
    private String distribution;          // alternate distribution methods for srun
    private String mailType;              // events that trigger emails
    private String mailUser;              // target email address
    private String mcsLabel;              // used with plugins
    private String mem;                   // real memory required per node (default units are megabytes)
    private String memPerCpu;             // minimum memory required per allocated CPU
    private String memPerGpu;             // minimum memory required per allocated GPU
    private String memBind;               // specify NUMA task/memory binding with affinity plugin
    private String minCpus;               // minimum number of logical cpus/processors per node
    private String nodes;                 // minimum number of allocated to job
    private String ntasks;                // maximum task job will launch
    private String network;               // network configuration
    private String nice;                  // nice value within slurm
    private boolean noRequeue;            // never restart or requeue job
    private String tasksPerCore;          // maximum tasks per core
    private String tasksPerGpu;           // tasks started per gpu
    private String tasksPerNode;          // tasks started per node
    private String tasksPerSocket;        // maximum tasks per socket
    private boolean overcommit;           // 1 job per node, or 1 task per cpu
    private String output;                // connect batch script's stdout/stderr to a file
    private String openMode;              // open output and error files with append or truncate
    private String parsable;              // output only job id and, if present, the cluster name
    private String partition;             // the queue name
    private String power;                 // power plugin options
    private String priority;              // request job priority
    private String profile;               // use one or more profiles
    private String propagate;             // propagate specified configurations to compute nodes
    private String qos;                   // quality of service
    private boolean reboot;               // reboot nodes
    private boolean requeue;              // allow job to be requeued
    private String reservation;           // allocate resources for the job from the named reservation
    private boolean oversubscribe;        // allocation can over-subscribe resources with other running jobs
    private String coreSpec;              // count of cores per node reserved by job for system operations
    private String signal;                // signal job when nearing end time
    private String socketsPerNode;        // minimum sockets per node
    private String spreadJob;             // spread job over maximum number of nodes
    private String switches;              // the maximum count of switches required for job allocation
    private String time;                  // set total run time limit
    private boolean testOnly;             // validate batch script and estimate queue time
    private String threadSec;             // count of specialized threads per node reserved by the job for system operations
    private String threadsPerCore;        // select nodes with at least the specified number of threads per core
    private String timeMin;               // minimum run time limit for the job
    private String tmp;                   // minimum amount of temporary disk space per node (default units are megabytes)
    private String uid;                   // attempt to submit and/or run a job as user instead of the invoking user id
    private String useMinNodes;           // when multiple ranges given, prefer the smaller counts
    private String nodeList;              // request a specific list of hosts
    private String waitAllNodes;          // wait to begin execution until all nodes are ready for use
    private String wckey;                 // specify wckey to be used with job
    private String exclude;               // explicitly exclude certain nodes
    
    // Slurm options not supported.
    //
    //  --chdir, --hold, --ignore-pbs, --nodes, --partition, --quiet
    //  --version, --verbose, --wait, --wrap, 
    //
    // Slurm options not supported or automatically set.
    //
    //  --hold, --ignore-pbs, --nodes, --partition, --quiet
    //  --version, --verbose, --wait, --wrap, 
    
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) {
        // TODO Auto-generated method stub
        return null;
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() {
        // TODO Auto-generated method stub
        return null;
    }
}
