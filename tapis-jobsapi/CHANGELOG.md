# Change Log for Tapis Jobs Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/jobs.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

-----------------------
## 1.2.2 - 2022-09-22

Fixes and preview feature release
### New features:

### Bug fixes:
1. Allow JobOutput listing for jobs that ran in shared app context

## 1.2.2 - 2022-08-23

Fixes and preview feature release

### Breaking Changes:


### New features:

### Bug fixes:
1. Added --force flag for docker container removal for FORK jobs on job cancel
2. Jobs OpenAPI spec schema fixed

## 1.2.1 - 2022-07-25

Fixes and preview feature release

### Breaking Changes:
1. Changed jobuuid parameter to jobUuid in resubmission APIs for consistency. 

### New features:
1. Job subscription APIs
2. Job event generation and transmission
3. Job shared history, resubmit request and output APIs
4. Updated 3rd party libraries

### Bug fixes:
1. Better test for batchqueue assignment to avoid NPE

-----------------------

## 1.2.0 - 2022-05-31

Maintenance release

### Breaking Changes:
- Refer to renamed notification subscription classes in shared library.

### New features:

### Bug fixes:

-----------------------

## 1.1.3 - 2022-05-09

Maintenance release

### Breaking Changes:
- none.

### New features:
1. Adjust JVM memory options and other deployment file clean up.
2. Improve JWT validation and authentication logging.

### Bug fixes:

-----------------------

## 1.1.2 - 2022-03-04

Java 17 upgrade

### Breaking Changes:
- none.

### New features:
1. Upgrade code and images to Java 17.0.2.
2. Generalized job event table to handle non-status events.
3. Added more throttling to Jobs service to better withstand job request bursts.
4. Tighten SK write secret endpoint validation.

### Bug fixes:

-----------------------

## 1.1.1 - 2022-02-01

Bug fix release

### Breaking Changes:
- none.

### Bug fixes:
1. Applied application limits to recovery as intended.
2. Adjusted how PENDING jobs are accounted for during recovery.
3. Fixed singularity --mount and --fusemount assignements.

-----------------------

## 1.1.0 - 2022-01-07

New minor release.

### Breaking Changes:
- none

### New features:
1. Fail-fast support for jobs that experience unrecoverable Docker errors.
2. Support for renamed task status enum in Apps client. 

-----------------------

## 1.0.6 - 2021-12-17

MPI and command prefix support.

### Breaking Changes:
- none

### New features:
1. MPI support.
2. Command prefix support.
3. Support for latest App and Systems interfaces.

### Bug fixes:
1. Hide/unhide flag fix.

-----------------------

## 1.0.5 - 2021-12-09

New job request interface release.

### Breaking Changes:
- JobType migration may be required.

### New features:
1. Job request args and inputs tweaks.
2. Use updated TapisSystem.getCanRunBatch() call.
3. Implemented JobType support.
4. Implemented batchSchedulerProfile support.
5. Implemented hide/unhide apis.

### Bug fixes:
1. Fix jobType assignment.
2. Fix openapi specification generation.

-----------------------

## 1.0.4 - 2021-11-10

New job request interface release.

### Breaking Changes:
- Job request interface changed.

### New features:
1. Added JSON schema for search request.
2. Support for new FileInputs design and interface.
3. Support for new FileInputArrays design and interface.
4. Support for new JobArgsSpec design and interface.
5. Support for new tapisLocal design and interface.
6. Updated job request tests.
7. Support for ArgInputModeEnum name change.

-----------------------

## 1.0.3 - 2021-10-12

Bug fix release.

### Breaking Changes:
- none

### New features:
1. Added ListFiles2 test program.

### Bug fixes:
1. Fix version in non-standard POM files.
2. Fixes to job search along with code clean up.
3. Fix includeLaunchFiles bug.

-----------------------

## 1.0.2 - 2021-09-17

Bug fix release.

### Breaking Changes:
- none

### Bug fixes:
1. Fix job listing conditional statement.
2. Fix TooManyFailures problem. 

-----------------------

## 1.0.1 - 2021-09-15

Incremental improvement and bug fix release.

### Breaking Changes:
- none

### New features:
1. Provided a default job description if one is not specified on job submission.

### Bug fixes:
1. Quoted environment variable values when they appear on command line
   for job submission to Slurm scheduler.
2. Account for file listing output pathnames that don't preserve the
   input specification's leading slash.
3. Fixed empty job listing case.
4. Added support for the singularity run --pwd option.
5. Added missing error message. 


-----------------------

## 1.0.0 - 2021-07-16

Initial release supporting basic CRUD operations on Tapis Job resources
as well as Job submission.

1. Zero-install remote job execution that uses SSH to process applications packaged in containers. 
2. Remote job lifecycle support including input staging, job staging, job execution, job monitoring
   and output archiving of user-specified container applications. 
3. Support for running Docker container applications on remote hosts.
4. Support for running Singularity container applications on remote hosts using either
   singularity start or singularity run.
5. Support for running Singularity container applications under Slurm.

### Breaking Changes:
- Initial release.

### New features:
 - Initial release.

### Bug fixes:
- None.
