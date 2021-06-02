package edu.utexas.tacc.tapis.jobs.stagers.singularityslurm;

import edu.utexas.tacc.tapis.jobs.stagers.singularitynative.SingularityRunCmd;
import edu.utexas.tacc.tapis.jobs.stagers.singularitynative.SingularityRunStager;
import edu.utexas.tacc.tapis.jobs.worker.execjob.JobExecutionContext;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

/** This class inherits from the singularity run stager class to access that
 * class's singularity run command member, which it initializes so that the
 * singularity run command can be generated.    
 * 
 * @author rcardone
 */
final class WrappedSingularityRunStager 
 extends SingularityRunStager
{
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    WrappedSingularityRunStager(JobExecutionContext jobCtx)
     throws TapisException
    {
        super(jobCtx);
    }

    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* getSingularityRunCmd:                                                  */
    /* ---------------------------------------------------------------------- */
    protected SingularityRunCmd getSingularityRunCmd() {return super.getSingularityRunCmd();}

    /* ---------------------------------------------------------------------- */
    /* getCmdTextWithEnvVars:                                                 */
    /* ---------------------------------------------------------------------- */
    protected String getCmdTextWithEnvVars() {return super.getCmdTextWithEnvVars();}
}
