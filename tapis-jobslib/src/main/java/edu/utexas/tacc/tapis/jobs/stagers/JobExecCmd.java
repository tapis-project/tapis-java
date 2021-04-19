package edu.utexas.tacc.tapis.jobs.stagers;

import edu.utexas.tacc.tapis.jobs.model.Job;

public interface JobExecCmd 
{
    /** Generate the container run command.  The generated text does not
     * contain any parameters to be passed to the application.  Depending
     * on the container runtime, the environment variable settings might
     * also be omitted.  If so, the generateEnv() method should be called
     * to create an environment variable input file.
     * 
     * @param job the executing job
     * @return the command with its options
     */
    String generateExecCmd(Job job);
    
    /** Generate the key/value assignments to be injected into a container's
     * runtime environment.  The format of the assignments are dictated by the
     * container runtime, but a list of key=value pairs separated by newline 
     * delimiters is typical since the content will ultimately end up in a
     * file.
     * 
     * @return the environment variable assignments
     */
    String generateEnvVarFileContent();
}
