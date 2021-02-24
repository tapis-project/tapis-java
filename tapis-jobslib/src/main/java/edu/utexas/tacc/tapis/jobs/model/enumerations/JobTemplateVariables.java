package edu.utexas.tacc.tapis.jobs.model.enumerations;

/** These template variables can be referenced using a ${varName} specification
 * when submitting a job.  These values will also be prefixed with "_tapis" and passed 
 * as environment variables into the application.  For example, the "JobName" template
 * variable value is assigned to the environment variable "_tapisJobName" when the 
 * application launches.
 * 
 * @author rcardone
 */
public enum JobTemplateVariables 
{
	JobName,
	JobUUID,
	Tenant,
	JobOwner,
	EffeciveUserId,
	
	AppId,
	AppVersion,
	
	ExecSystemId,
	ExecSystemExecDir,
	ExecSystemInputDir,
	ExecSystemOutputDir,
	DynamicExecSystem,
	JobWorkingDir,
	
	ExecSystemLogicalQueue,
	ExecSystemHPCQueue,
	
	ArchiveSystemId,
	ArchiveSystemDir,
	ArchiveOnAppError,
	
	DtnSystemId,
	DtnMountPoint,
	DtnMountSourcePath,
	
	Nodes,
	CoresPerNode,
	MemoryMB,
	MaxMinutes,

	SysBucketName,
	SysRootDir,
	SysHost,
	SysBatchScheduler,
	
	JobCreateTime,
	JobCreateDate,
	JobCreateTimestamp
}
