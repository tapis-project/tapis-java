package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.stagers.AbstractJobExecStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionUtils;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.recoverable.TapisServiceConnectionException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;

public abstract class AbstractSingularityStager 
 extends AbstractJobExecStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(AbstractSingularityStager.class);

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected AbstractSingularityStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        super(jobCtx);
    }

    /* ---------------------------------------------------------------------- */
    /* makeEnvFilePath:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Write the environment variables to a host file.
     * 
     * @throws TapisImplException 
     * @throws TapisServiceConnectionException 
     */
    protected String makeEnvFilePath() 
     throws TapisException
    {
        // Put the env file in the execution directory.
        var fm = _jobCtx.getJobFileManager();
        return fm.makeAbsExecSysExecPath(JobExecutionUtils.JOB_ENV_FILE);
    }

    /* ---------------------------------------------------------------------- */
    /* assignCmd:                                                             */
    /* ---------------------------------------------------------------------- */
    /** Save the user-specified singularity parameter.  This method set the 
     * singularity options that are common to both singularity run and start.
     * 
     * @param singularityCmd the start or run command
     * @param option the singularity argument
     * @param value the argument's non-null value
     */
    protected void assignCmd(AbstractSingularityExecCmd singularityCmd, String option, String value)
     throws JobException
    {
        switch (option) {
            // Start/Run common options.
            case "--add-caps":
                singularityCmd.setCapabilities(value);
                break;
            case "--bind":
            case "-B":
                singularityCmd.setBind(value);
                break;
            case "--cleanenv":
            case "-e":
                singularityCmd.setCleanEnv(true);
                break;
            case "--contain":
            case "-c":
                singularityCmd.setContain(true);
                break;
            case "--containall":
            case "-C":
                singularityCmd.setContainAll(true);
                break;
            case "--disable-cache":
                singularityCmd.setDisableCache(true);
                break;
            case "--dns":
                singularityCmd.setDns(value);
                break;
            case "--drop-caps":
                singularityCmd.setDropCapabilities(value);
                break;
            case "--home":
            case "-H":
                singularityCmd.setHome(value); 
                break;
            case "--hostname":
                singularityCmd.setHostname(value);
                break;
            case "--net":
            case "-n":
                singularityCmd.setNet(true);
                break;
            case "--network":
                singularityCmd.setNetwork(value);
                break;
            case "--network-args":
                isAssigned("singularity", option, value);
                singularityCmd.getNetworkArgs().add(value);
                break;
            case "--no-home":
                singularityCmd.setNoHome(true);
                break;
            case "--no-init":
                singularityCmd.setNoInit(true);
                break;
            case "--no-mount":
                isAssigned("singularity", option, value);
                singularityCmd.getNoMounts().add(value);
                break;
            case "--no-privs":
                singularityCmd.setNoPrivs(true);
                break;
            case "--no-umask":
                singularityCmd.setNoUMask(true);
                break;
            case "--nohttps":
                singularityCmd.setNoHTTPS(true);
                break;
            case "--nv":
                singularityCmd.setNv(true);
                break;
            case "--overlay":
            case "-O":
                isAssigned("singularity", option, value);
                singularityCmd.getOverlay().add(value);
                break;
            case "--pem-path":
                singularityCmd.setPemPath(value);
                break;
            case "--rocm":
                singularityCmd.setRocm(true);
                break;
            case "--scratch":
            case "-S":
                isAssigned("singularity", option, value);
                singularityCmd.getScratch().add(value);
                break;
            case "--security":
                isAssigned("singularity", option, value);
                singularityCmd.getSecurity().add(value);
                break;
            case "--userns":
            case "-U":
                singularityCmd.setUserNs(true);
                break;
            case "--uts":
                singularityCmd.setUts(true);
                break;
            case "--workdir":
            case "-W":
                singularityCmd.setWorkdir(value);
                break;
            case "--writable":
            case "-w":
                singularityCmd.setWritable(true);
                break;
            case "--writable-tmpfs":
                singularityCmd.setWritableTmpfs(true);
                break;
                
            default:
                // The following options are reserved for tapis-only use.
                // If the user specifies any of them as a container option,
                // the job will abort.  Note that environment variables are 
                // passed in via their own ParameterSet object.
                //
                //   --pidfile, --env
                //
                String msg = MsgUtils.getMsg("JOBS_CONTAINER_UNSUPPORTED_ARG", "singularity", option);
                throw new JobException(msg);
        }
    }
}
