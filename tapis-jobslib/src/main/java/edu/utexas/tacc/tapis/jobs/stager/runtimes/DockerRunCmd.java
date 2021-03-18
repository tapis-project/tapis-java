package edu.utexas.tacc.tapis.jobs.stager.runtimes;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public final class DockerRunCmd 
 implements RunCmd
{
// docker run --name SleepSeconds --rm -u "$(id -u):$(id -g)" ${vmounts} ${envdirs} ${jobparms} -e 'MAIN_CLASS=edu.utexas.tacc.testapps.tapis.SleepSeconds' tapis/testapps:main 
    
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    private String                    addHost;
    private List<AttachEnum>          attachList;
    private List<BindMount>           bindMount;
    private String                    cidFile;
    private String                    cpus;
    private String                    cpusetCPUs;
    private String                    cpusetMEMs;
    private List<Pair<String,String>> env = new ArrayList<Pair<String,String>>();
    private GpuOptions                gpus;
    private List<String>              groups;
    private String                    hostName;
    private String                    ip;
    private List<Pair<String,String>> labels;
    private String                    logDriver;
    private List<Pair<String,String>> logOpts;
    private String                    memory;
    private String                    name;
    private String                    network;
    private List<Port>                portMappings;
    private boolean                   rm;
    private List<TmpfsMount>          tmpfsMount;
    private String                    user;
    private List<VolumMount>          volumeMount;
    private String                    workdir;
    
    // Valid values for the attach option.
    public enum AttachEnum {stdin, stdout, stderr}
    
    // Port protocols.
    public enum TransportProcol {tcp, udp, sctp}
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateRunCmd:                                                        */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateRunCmd() 
    {
        return null;
    }
    
    /* ********************************************************************** */
    /*                           BindMount Class                              */
    /* ********************************************************************** */
    public static final class BindMount
    {
        private String  source;
        private String  target;
        private boolean readOnly;
        
        public String getSource() {return source;}
        public void setSource(String source) {this.source = source;}
        public String getTarget() {return target;}
        public void setTarget(String target) {this.target = target;}
        public boolean isReadOnly() {return readOnly;}
        public void setReadOnly(boolean readOnly) {this.readOnly = readOnly;}
    }

    /* ********************************************************************** */
    /*                          TmpfsMount Class                              */
    /* ********************************************************************** */
    public static final class TmpfsMount
    {
        private String source;
        private String target;
        private String size; // unlimited size by default
        private String mode; // 1777 octal - world writable w/sticky bit by default
        
        public String getSource() {return source;}
        public void setSource(String source) {this.source = source;}
        public String getTarget() {return target;}
        public void setTarget(String target) {this.target = target;}
        public String getSize() {return size;}
        public void setSize(String size) {this.size = size;}
        public String getMode() {return mode;}
        public void setMode(String mode) {this.mode = mode;}
    }
    
    /* ********************************************************************** */
    /*                          VolumMount Class                              */
    /* ********************************************************************** */
    public static final class VolumMount
    {
        private String                    source;
        private String                    target;
        private boolean                   readonly;
        private List<Pair<String,String>> keyValueList = new ArrayList<Pair<String,String>>();
        
        public String getSource() {return source;}
        public void setSource(String source) {this.source = source;}
        public String getTarget() {return target;}
        public void setTarget(String target) {this.target = target;}
        public boolean isReadonly() {return readonly;}
        public void setReadonly(boolean readonly) {this.readonly = readonly;}
        public List<Pair<String,String>> getKeyValueList() {return keyValueList;}
        public void setKeyValueList(List<Pair<String,String>> keyValueList) {
            this.keyValueList = keyValueList;
        }
    }

    /* ********************************************************************** */
    /*                          VolumMount Class                              */
    /* ********************************************************************** */
    public static final class Port
    {
        private String          hostInterface;
        private String          hostPort;
        private String          containerPort;
        private TransportProcol protocol;
        
        public String getHostInterface() {return hostInterface;}
        public void setHostInterface(String hostInterface) {this.hostInterface = hostInterface;}
        public String getHostPort() {return hostPort;}
        public void setHostPort(String hostPort) {this.hostPort = hostPort;}
        public String getContainerPort() {return containerPort;}
        public void setContainerPort(String containerPort) {this.containerPort = containerPort;}
        public TransportProcol getProtocol() {return protocol;}
        public void setProtocol(TransportProcol protocol) {this.protocol = protocol;}
    }
    
    /* ********************************************************************** */
    /*                          VolumMount Class                              */
    /* ********************************************************************** */
    public static final class GpuOptions
    {
        private String options;

        public String getOptions() {return options;}
        public void setOptions(String options) {this.options = options;}
    }

    /* ********************************************************************** */
    /*                          Top-Level Accessors                           */
    /* ********************************************************************** */
    public String getAddHost() {
        return addHost;
    }

    public void setAddHost(String addHost) {
        this.addHost = addHost;
    }

    public List<AttachEnum> getAttachList() {
        if (attachList == null) attachList = new ArrayList<AttachEnum>();
        return attachList;
    }

    public void setAttachList(List<AttachEnum> attachList) {
        this.attachList = attachList;
    }

    public List<BindMount> getBindMount() {
        if (bindMount == null) bindMount = new ArrayList<BindMount>();
        return bindMount;
    }

    public void setBindMount(List<BindMount> bindMount) {
        this.bindMount = bindMount;
    }

    public String getCidFile() {
        return cidFile;
    }

    public void setCidFile(String cidFile) {
        this.cidFile = cidFile;
    }

    public String getCpus() {
        return cpus;
    }

    public void setCpus(String cpus) {
        this.cpus = cpus;
    }

    public String getCpusetCPUs() {
        return cpusetCPUs;
    }

    public void setCpusetCPUs(String cpusetCPUs) {
        this.cpusetCPUs = cpusetCPUs;
    }

    public String getCpusetMEMs() {
        return cpusetMEMs;
    }

    public void setCpusetMEMs(String cpusetMEMs) {
        this.cpusetMEMs = cpusetMEMs;
    }

    public List<Pair<String, String>> getEnv() {
        // Initialized on construction.
        return env;
    }

    public void setEnv(List<Pair<String, String>> env) {
        this.env = env;
    }

    public GpuOptions getGpus() {
        return gpus;
    }

    public void setGpus(GpuOptions gpus) {
        this.gpus = gpus;
    }

    public List<String> getGroups() {
        if (groups == null) groups = new ArrayList<String>();
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public List<Pair<String, String>> getLabels() {
        if (labels == null) labels = new ArrayList<Pair<String,String>>();
        return labels;
    }

    public void setLabels(List<Pair<String, String>> labels) {
        this.labels = labels;
    }

    public String getLogDriver() {
        return logDriver;
    }

    public void setLogDriver(String logDriver) {
        this.logDriver = logDriver;
    }

    public List<Pair<String, String>> getLogOpts() {
        if (logOpts == null) logOpts = new ArrayList<Pair<String,String>>();
        return logOpts;
    }

    public void setLogOpts(List<Pair<String, String>> logOpts) {
        this.logOpts = logOpts;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public List<Port> getPortMappings() {
        if (portMappings == null) portMappings = new ArrayList<Port>();
        return portMappings;
    }

    public void setPortMappings(List<Port> portMappings) {
        this.portMappings = portMappings;
    }

    public boolean isRm() {
        return rm;
    }

    public void setRm(boolean rm) {
        this.rm = rm;
    }

    public List<TmpfsMount> getTmpfsMount() {
        if (tmpfsMount == null) tmpfsMount = new ArrayList<TmpfsMount>();
        return tmpfsMount;
    }

    public void setTmpfsMount(List<TmpfsMount> tmpfsMount) {
        this.tmpfsMount = tmpfsMount;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<VolumMount> getVolumeMount() {
        if (volumeMount == null) volumeMount = new ArrayList<VolumMount>();
        return volumeMount;
    }

    public void setVolumeMount(List<VolumMount> volumeMount) {
        this.volumeMount = volumeMount;
    }

    public String getWorkdir() {
        return workdir;
    }

    public void setWorkdir(String workdir) {
        this.workdir = workdir;
    }
}
