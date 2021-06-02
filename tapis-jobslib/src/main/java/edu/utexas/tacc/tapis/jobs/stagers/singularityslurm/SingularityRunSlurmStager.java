package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.stagers.singularitynative.SingularityRunStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

public final class SingularityRunSlurmStager 
  extends SingularityRunStager
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(SingularityRunSlurmStager.class);

    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Slurm run command object.
    private final SingularityRunSlurmCmd _slurmRunCmd;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    protected SingularityRunSlurmStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        super(jobCtx);
        _slurmRunCmd = configureSlurmRunCmd();
    }

    /* ---------------------------------------------------------------------- */
    /* generateWrapperScript:                                                 */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateWrapperScript() throws TapisException {
        // TODO Auto-generated method stub
        return null;
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFile:                                                    */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String generateEnvVarFile() throws TapisException {
        // TODO Auto-generated method stub
        return null;
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* configureSlurmRunCmd:                                                  */
    /* ---------------------------------------------------------------------- */
    private SingularityRunSlurmCmd configureSlurmRunCmd()
     throws TapisException
    {
        var runCmd = new SingularityRunSlurmCmd();
        
        return runCmd;
    }
}
