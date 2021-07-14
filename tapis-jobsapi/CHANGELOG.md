# Change Log for Tapis Jobs Service

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/jobs.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

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
