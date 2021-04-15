package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;

public final class SingularityStartCmd 
 extends AbstractSingularityExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Fields specific to instance start. 
    private String                    pidFile;      // Tapis hardcoded file path for instance pid
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Start filling in the options that are tapis-only assigned.
        buf.append("singularity instance start");
        if (!getEnv().isEmpty()) {
            buf.append(" --env-file ");
            buf.append(getEnvFile());
        }
        buf.append(" --pid-file ");
        buf.append(getPidFile());
        
        // ------ Fill in the common user-specified arguments.
        addCommonExecArgs(buf);
        
        // ------ Fill in command-specific user-specified arguments.
        if (StringUtils.isNotBlank(getPidFile())) {
            buf.append(" --pid-file ");
            buf.append(getPidFile());
        }
        
        // ------ Assign image.
        buf.append(" ");
        buf.append(getImage());
        
        // ------ Assign application arguments.
        
        // ------ Assign instance name.
        buf.append(" ");
        buf.append(job.getUuid());

        return buf.toString();
    }

    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
        // This should never happen since tapis variables are always specified.
        if (getEnv().isEmpty()) return null;
        
        // Create the key=value records, one per line.
        return getPairListArgs(getEnv());
    }

    /* ********************************************************************** */
    /*                            Private Methods                             */
    /* ********************************************************************** */
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    public String getPidFile() {
        return pidFile;
    }

    public void setPidFile(String pidFile) {
        this.pidFile = pidFile;
    }

}
