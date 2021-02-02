package edu.utexas.tacc.tapis.jobs.killers;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.systems.client.gen.model.TSystem;

/**
 * Factory to init job killers
 * 
 * @author dooley
 * 
 */
public class JobKillerFactory {

	public static JobKiller getInstance(Job job, TSystem executionSystem) 
	{
	    return null; // TODO:
	    // kill the condor job
//		if (executionSystem.getExecutionType().equals(ExecutionType.CONDOR)) { 
//			return new CondorKiller(job, executionSystem);
//		}
//		// or kill the batch job
//		else if (executionSystem.getExecutionType().equals(ExecutionType.HPC)) {
//		    return new HPCKiller(job, executionSystem);
//        }
//		// or try to kill the forked process
//		else {
//		    return new ProcessKiller(job, executionSystem);
//		}
	}
}
