package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** These template variables can be referenced using a ${varName} specification
 * when submitting a job.  These values will also be passed as environment variables
 * into the application.
 * 
 * @author rcardone
 */
public enum JobTemplateVariables 
{
	tapisJobName,
	tapisJobId,
	tapisTenant,
	tapisJobOwner,
	tapisEffeciveUserId,
	tapisAppId,
	tapisAppVersion,
	tapisExecSystemId,
	tapisArchiveSystemId,
	tapisExecSystemExecDir,
	tapisExecSystemInputDir,
	tapisExecSystemOutputDir,
	tapisExecSystemLogicalQueue,
	tapisExecSystemHPCQueue,
	tapisArchiveOnAppError,
	
	tapisDtnSystemId,
	tapisDtnMountPoint,
	tapisDtnSubDir,
	
	tapisNodes,
	tapisCoresPerNode,
	tapisMemoryMB,
	tapisMaxMinutes,

	tapisSysWorkingDir,
	tapisSysBucketName,
	tapisSysRootDir,
	tapisSysHost,
	tapisSysDTNSystemId,
	
	tapisTime,
	tapisDate,
	tapisTimestamp
}
