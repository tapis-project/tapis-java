package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** These template variables can be referenced using a ${varName} specification
 * when submitting a job.  These values will also be passed as environment variables
 * into the application.
 * 
 * @author rcardone
 */
public enum JobTemplateVariables 
{
	_tapisJobName,
	_tapisJobUUID,
	_tapisTenant,
	_tapisJobOwner,
	_tapisEffeciveUserId,
	
	_tapisAppId,
	_tapisAppVersion,
	
	_tapisExecSystemId,
	_tapisExecSystemExecDir,
	_tapisExecSystemInputDir,
	_tapisExecSystemOutputDir,
	_tapisDynamicExecSystem,
	_tapisJobWorkingDir,
	
	_tapisExecSystemLogicalQueue,
	_tapisExecSystemHPCQueue,
	
	_tapisArchiveSystemId,
	_tapisArchiveSystemDir,
	_tapisArchiveOnAppError,
	
	_tapisDtnSystemId,
	_tapisDtnMountPoint,
	_tapisDtnMountSourcePath,
	
	_tapisNodes,
	_tapisCoresPerNode,
	_tapisMemoryMB,
	_tapisMaxMinutes,

	_tapisSysBucketName,
	_tapisSysRootDir,
	_tapisSysHost,
	_tapisSysBatchScheduler,
	
	_tapisJobCreateTime,
	_tapisJobCreateDate,
	_tapisJobCreateTimestamp
}
