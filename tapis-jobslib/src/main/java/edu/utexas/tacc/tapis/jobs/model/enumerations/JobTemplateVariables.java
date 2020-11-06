package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** These template variables can be referenced using a ${varName} specification
 * when submitting a job.  These values will also be 
 * 
 * @author rcardone
 */
public enum JobTemplateVariables 
{
	tapisJobName,
	tapisJobId,
	tapisTenant,
	topisJobOwner,
	tapisAppId,
	tapisAppVersion,
	tapisExecSystemId,
	tapisArchiveSystemId,
	tapisNodes,
	tapisCoresPerNode,
	tapisMemoryMB,
	tapisMaxMinutes
}
