package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.model.Job;

public final class SingularityRunCmd 
 extends AbstractSingularityExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // Fields specific to instance start. 
    private String          app;      // an application to run inside a container
    private boolean         ipc;      // run container in a new IPC namespace
    private boolean         noNet;    // disable VM network handling
    private boolean         pid;      // run container in a new PID namespace
    private boolean         vm;       // enable VM support
    private String          vmCPU;    // number of CPU cores to allocate to Virtual Machine
    private boolean         vmErr;    // enable attaching stderr from VM
    private String          vmIP;     // IP Address to assign for container usage, default is DHCP in bridge network
    private String          vmRAM;    // amount of RAM in MiB to allocate to Virtual Machine (default "1024")
    
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
        buf.append("nohup singularity run");
        if (!getEnv().isEmpty()) {
            buf.append(" --env-file ");
            buf.append(getEnvFile());
        }
        
        // ------ Fill in the common user-specified arguments.
        addCommonExecArgs(buf);
        
        // ------ Fill in command-specific user-specified arguments.
        if (StringUtils.isNotBlank(getApp())) {
            buf.append(" --app ");
            buf.append(getApp());
        }

        if (isIpc()) buf.append(" --ipc");
        if (isNoNet()) buf.append(" --nonet");
        if (isPid()) buf.append(" --pid");
        if (isVm()) buf.append(" --vm");

        if (StringUtils.isNotBlank(getVmCPU())) {
            buf.append(" --vm-cpu ");
            buf.append(getVmCPU());
        }
        if (isVmErr()) buf.append(" --vm-err");
        if (StringUtils.isNotBlank(getVmIP())) {
            buf.append(" --vm-ip ");
            buf.append(getVmIP());
        }
        if (StringUtils.isNotBlank(getVmRAM())) {
            buf.append(" --vm-ram ");
            buf.append(getVmRAM());
        }
        
        // ------ Assign image.
        buf.append(" ");
        buf.append(getImage());

        // ------ Assign application arguments.
        
        // ------ Run as a background command.
        buf.append(" &");

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
    /*                               Accessors                                */
    /* ********************************************************************** */
    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public boolean isIpc() {
        return ipc;
    }

    public void setIpc(boolean ipc) {
        this.ipc = ipc;
    }

    public boolean isNoNet() {
        return noNet;
    }

    public void setNoNet(boolean noNet) {
        this.noNet = noNet;
    }

    public boolean isPid() {
        return pid;
    }

    public void setPid(boolean pid) {
        this.pid = pid;
    }

    public boolean isVm() {
        return vm;
    }

    public void setVm(boolean vm) {
        this.vm = vm;
    }

    public String getVmCPU() {
        return vmCPU;
    }

    public void setVmCPU(String vmCPU) {
        this.vmCPU = vmCPU;
    }

    public boolean isVmErr() {
        return vmErr;
    }

    public void setVmErr(boolean vmErr) {
        this.vmErr = vmErr;
    }

    public String getVmIP() {
        return vmIP;
    }

    public void setVmIP(String vmIP) {
        this.vmIP = vmIP;
    }

    public String getVmRAM() {
        return vmRAM;
    }

    public void setVmRAM(String vmRAM) {
        this.vmRAM = vmRAM;
    }
}
