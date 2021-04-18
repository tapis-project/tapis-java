package edu.utexas.tacc.tapis.jobs.stagers.dockernative;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;

/** This class stores the command line options for the docker run command that executes
 * an application's container.  The general approach is to take the user-specified text
 * as is.  Validation and reformatting kept to a minimum.  More rigorous parsing and 
 * validation can be added if the need arises, but this approach is simple to implement
 * and maintain.
 * 
 * @author rcardone
 */
public final class DockerRunCmd 
 implements JobExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List fields that we always populate are initialized on construction,
    // all others are initialized on demand.
    private String                    addHost;
    private String                    cidFile;
    private String                    cpus;
    private String                    cpusetCPUs;
    private String                    cpusetMEMs;
    private List<Pair<String,String>> env = new ArrayList<Pair<String,String>>();
    private String                    envFile;
    private String                    gpus;
    private List<String>              groups;
    private String                    hostName;
    private String                    ip;
    private String                    ip6;
    private String                    image;
    private List<Pair<String,String>> labels;
    private String                    logDriver;
    private String                    logOpts;
    private String                    memory;
    private List<String>              mount = new ArrayList<String>();
    private String                    name;
    private String                    network;
    private String                    networkAlias;
    private List<String>              portMappings;
    private boolean                   rm;
    private List<String>              tmpfs;
    private String                    user;
    private List<String>              volumeMount;
    private String                    workdir;
    
    // Arguments passed to application, which always begin with a space character.
    private String                    appArguments; 
    
    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* generateExecCmd:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateExecCmd(Job job) 
    {
        // The generated wrapper script will contain a docker run command: 
        //
        //   docker run [OPTIONS] IMAGE[:TAG|@DIGEST] [COMMAND] [ARG...]
        
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // ------ Start filling in the options that are tapis-only assigned.
        buf.append("# Issue docker run command and write container ID to file.\n");
        buf.append("# Format: docker run [options] image[:tag|@digest] [app args]\n");
        
        buf.append("docker run -d --name ");
        buf.append(name);
        buf.append(" --user ");
        buf.append(user);
        buf.append(" --cidfile ");
        buf.append(cidFile);
        buf.append(" --env-file ");
        buf.append(envFile);
        if (rm) buf.append(" --rm");
        
        // ------ Fill in the options that the user may have set.
        if (addHost != null) {
            buf.append(" --addhost ");
            buf.append(addHost);
        }
        if (cpus != null) {
            buf.append(" --cpus ");
            buf.append(cpus);
        }
        if (cpusetCPUs != null) {
            buf.append(" --cpuset-cpus ");
            buf.append(cpusetCPUs);
        }
        if (cpusetMEMs != null) {
            buf.append(" --cpuset-mems ");
            buf.append(cpusetMEMs);
        }
        if (gpus != null) {
            buf.append(" --gpus ");
            buf.append(gpus);
        }
        if (groups != null) {
            for (var s : groups) {
                buf.append(" --group-add ");
                buf.append(s);
            }
        }
        if (hostName != null) {
            buf.append(" --hostname ");
            buf.append(hostName);
        }
        if (ip != null) {
            buf.append(" --ip ");
            buf.append(ip);
        }
        if (ip6 != null) {
            buf.append(" --ip6 ");
            buf.append(ip6);
        }
        if (labels != null) {
            for (var s : labels) {
                buf.append(" --label ");
                buf.append(s);
            }
        }
        if (logDriver != null) {
            buf.append(" --log-driver ");
            buf.append(logDriver);
        }
        if (logOpts != null) {
            buf.append(" --log-opt ");
            buf.append(logOpts);
        }
        if (memory != null) {
            buf.append(" --memory ");
            buf.append(memory);
        }
        if (network != null) {
            buf.append(" --network ");
            buf.append(network);
        }
        if (networkAlias != null) {
            buf.append(" --network-alias ");
            buf.append(networkAlias);
        }
        if (portMappings != null) {
            for (var s : portMappings) {
                buf.append(" -p ");
                buf.append(s);
            }
        }
        if (workdir != null) {
            buf.append(" --workdir ");
            buf.append(workdir);
        }
        
        // ------ Assign the volume mounts.
        for (var s : mount) {
            buf.append(" --mount ");
            buf.append(s);
        }
        if (tmpfs != null) {
            for (var s : tmpfs) {
                buf.append(" --tmpfs ");
                buf.append(s);
            }
        }
        if (volumeMount != null) {
            for (var s : volumeMount) {
                buf.append(" --volume ");
                buf.append(s);
            }
        }
        
        // ------ Append the image.
        buf.append(" ");
        buf.append(image);
        
        // ------ Append the application arguments.
        if (!StringUtils.isBlank(appArguments))
            buf.append(appArguments); // begins with space char
        
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* generateEnvVarFileContent:                                             */
    /* ---------------------------------------------------------------------- */
    @Override
    public String generateEnvVarFileContent() 
    {
        // Create the command buffer.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // Write each assignment to the buffer.
        for (var pair : env) {
            // The key always starts the line.
            buf.append(pair.getKey());
            
            // Are we going to use the short or long form?
            // The short form is just the name of an environment variable
            // that docker will import into the container ONLY IF it exists
            // in the environment from which docker is called.  The long 
            // form is key=value.  Note that we don't escape characters in 
            // the value.
            var value = pair.getValue();
            if (value != null && !value.isEmpty()) {
                // The long form forces an explicit assignment.
                buf.append("=");
                buf.append(pair.getValue());
            }
            buf.append("\n");
        }
        
        return buf.toString();
    }
    
    /* ********************************************************************** */
    /*                           BindMount Class                              */
    /* ********************************************************************** */
    public static final class BindMount
     extends AbstractDockerMount
    {
        private boolean readOnly;
        
        public BindMount() {super(MountType.bind);}
        
        public boolean isReadOnly() {return readOnly;}
        public void setReadOnly(boolean readOnly) {this.readOnly = readOnly;}
        
        @Override
        public String toString()
        {
            // Construct the value of a bind mount (i.e. everything
            // other than the --mount flag).
            final int capacity = 256;
            StringBuilder buf = new StringBuilder(capacity);
            buf.append("type=");
            buf.append(type.name());
            buf.append(",source=");
            buf.append(source);
            buf.append(",target=");
            buf.append(target);
            if (readOnly) buf.append(",readonly");
            
            return buf.toString();
        }
    }

    /* ********************************************************************** */
    /*                          TmpfsMount Class                              */
    /* ********************************************************************** */
    public static final class TmpfsMount
     extends AbstractDockerMount
    {
        private String size; // unlimited size by default
        private String mode; // 1777 octal - world writable w/sticky bit by default
        
        public TmpfsMount() {super(MountType.tmpfs);}
        
        public String getSize() {return size;}
        public void setSize(String size) {this.size = size;}
        public String getMode() {return mode;}
        public void setMode(String mode) {this.mode = mode;}
    }
    
    /* ********************************************************************** */
    /*                          VolumeMount Class                             */
    /* ********************************************************************** */
    public static final class VolumeMount
     extends AbstractDockerMount
    {
        private boolean                   readonly;
        private List<Pair<String,String>> keyValueList = new ArrayList<Pair<String,String>>();
        
        public VolumeMount() {super(MountType.volume);}
        
        public boolean isReadonly() {return readonly;}
        public void setReadonly(boolean readonly) {this.readonly = readonly;}
        public List<Pair<String,String>> getKeyValueList() {return keyValueList;}
        public void setKeyValueList(List<Pair<String,String>> keyValueList) {
            this.keyValueList = keyValueList;
        }
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

    public String getEnvFile() {
        return envFile;
    }

    public void setEnvFile(String envFile) {
        this.envFile = envFile;
    }

    public String getGpus() {
        return gpus;
    }

    public void setGpus(String gpus) {
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

    public String getIp6() {
        return ip6;
    }

    public void setIp6(String ip6) {
        this.ip6 = ip6;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
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

    public String getLogOpts() {
        return logOpts;
    }

    public void setLogOpts(String logOpts) {
        this.logOpts = logOpts;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public List<String> getMount() {
        return mount;
    }

    public void setMount(List<String> mount) {
        this.mount = mount;
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

    public String getNetworkAlias() {
        return networkAlias;
    }

    public void setNetworkAlias(String networkAlias) {
        this.networkAlias = networkAlias;
    }

    public List<String> getPortMappings() {
        if (portMappings == null) portMappings = new ArrayList<String>();
        return portMappings;
    }

    public void setPortMappings(List<String> portMappings) {
        this.portMappings = portMappings;
    }

    public boolean isRm() {
        return rm;
    }

    public void setRm(boolean rm) {
        this.rm = rm;
    }

    public List<String> getTmpfs() {
        if (tmpfs == null) tmpfs = new ArrayList<String>();
        return tmpfs;
    }

    public void setTmpfs(List<String> tmpfs) {
        this.tmpfs = tmpfs;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<String> getVolumeMount() {
        if (volumeMount == null) volumeMount = new ArrayList<String>();
        return volumeMount;
    }

    public void setVolumeMount(List<String> volumeMount) {
        this.volumeMount = volumeMount;
    }

    public String getWorkdir() {
        return workdir;
    }

    public void setWorkdir(String workdir) {
        this.workdir = workdir;
    }

    public String getAppArguments() {
        return appArguments;
    }

    public void setAppArguments(String appArguments) {
        this.appArguments = appArguments;
    }
}
