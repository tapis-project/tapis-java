package edu.utexas.tacc.tapis.jobs.stagers.singularitynative;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import edu.utexas.tacc.tapis.jobs.stagers.JobExecCmd;

public abstract class AbstractSingularityExecCmd 
  implements JobExecCmd
{
    /* ********************************************************************** */
    /*                                Fields                                  */
    /* ********************************************************************** */
    // List fields that we always populate are initialized on construction,
    // all others are initialized on demand.
    private String                    capabilities; // comma separated list
    private String                    bind;         // comma separated list of src[:dest[:opts]]
    private boolean                   cleanEnv;     // clean environment before running container
    private boolean                   contain;      // use minimal /dev and empty other directories
    private boolean                   containAll;   // contain file systems and also PID, IPC, and environment
    private boolean                   disableCache; // don't read or write cache
    private String                    dns;          // list of DNS servers separated by commas to add in resolv.conf
    private String                    dropCapabilities; // a comma separated capability list to drop
    private List<Pair<String,String>> env;          // pass environment variable to contained process
    private String                    envFile;      // file of key=value environment assignments
    private String                    home;         // either be a src path or src:dest pair
    private String                    hostname;     // set container host name
    private String                    image;        // the full image specification
    private boolean                   net;          // run container in a new network namespace
    private String                    network;      // network type separated by commas
    private List<String>              networkArgs;  // network arguments to pass to CNI plugins
    private boolean                   noHome;       // do NOT mount users home directory
    private boolean                   noInit;       // disable one or more mount xxx options set in singularity.conf
    private List<String>              noMounts;     // disable one or more mount xxx options set in singularity.conf
    private boolean                   noPrivs;      // drop all privileges from root user in container
    private boolean                   noUMask;      // do not propagate umask to the container, set default 0022 umask
    private boolean                   noHTTPS;      // do NOT use HTTPS with the docker:// transport
    private boolean                   nv;           // enable experimental Nvidia support
    private List<String>              overlay;      // use an overlayFS image for persistent data storage
    private String                    pemPath;      // enter an path to a PEM formated RSA key for an encrypted container
    private boolean                   rocm;         // enable experimental Rocm support
    private List<String>              scratch;      // include a scratch directory in container linked to a temporary dir
    private List<String>              security;     // enable security features (SELinux, Apparmor, Seccomp)
    private boolean                   userNs;       // run container in a new user namespace
    private boolean                   uts;          // run container in a new UTS namespace
    private String                    workdir;      // working directory to be used for /tmp, /var/tmp and $HOME (if --contain was also used)
    private boolean                   writable;     // This option makes the container file system accessible as read/write
    private boolean                   writableTmpfs;// makes the file system accessible as read-write with non persistent data (with overlay support only)

    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    public AbstractSingularityExecCmd()
    {
        // Lists we know are not going to be empty.
        env = new ArrayList<Pair<String,String>>();
    }
    
    /* ********************************************************************** */
    /*                           Protected Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* addCommonExecArgs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Fill in container arguments common to both start and run.  Note that
     * env and envFile are processed separately by subclasses.
     * 
     * @param buf the buffer accumulating are argument assignments 
     */
    protected void addCommonExecArgs(StringBuilder buf)
    {
        // ------ Fill in user-supplied options common to start and run.
        if (StringUtils.isNotBlank(getCapabilities())) {
            buf.append(" --add-caps ");
            buf.append(getCapabilities());
        }
        if (StringUtils.isNotBlank(getBind())) {
            buf.append(" --bind ");
            buf.append(getBind());
        }
        if (isCleanEnv()) buf.append(" --cleanenv");
        if (isContain()) buf.append(" --contain");
        if (isContainAll()) buf.append(" --containall");
        if (isDisableCache()) buf.append(" --disable-cache");
        
        if (StringUtils.isNotBlank(getDns())) {
            buf.append(" --dns ");
            buf.append(getDns());
        }
        if (StringUtils.isNotBlank(getDropCapabilities())) {
            buf.append(" --drop-caps ");
            buf.append(getDropCapabilities());
        }
        
        if (StringUtils.isNotBlank(getHome())) {
            buf.append(" --home ");
            buf.append(getHome());
        }
        if (StringUtils.isNotBlank(getHostname())) {
            buf.append(" --hostname ");
            buf.append(getHostname());
        }
        
        if (isNet()) buf.append(" --net");
        if (StringUtils.isNotBlank(getNetwork())) {
            buf.append(" --network ");
            buf.append(getNetwork());
        }
        if (!networkArgsIsNull() && !getNetworkArgs().isEmpty()) 
            buf.append(getStringListArgs(" --network-args ", getNetworkArgs()));
        
        if (isNoHome()) buf.append(" --no-home");
        if (isNoInit()) buf.append(" --no-init");
        if (!noMountsIsNull() && !getNoMounts().isEmpty()) 
            buf.append(getStringListArgs(" --no-mount ", getNoMounts()));
        if (isNoPrivs()) buf.append(" --no-privs");
        if (isNoUMask()) buf.append(" --no-umask");
        if (isNoHTTPS()) buf.append(" --nohttps");
        if (isNv()) buf.append(" --nv");
        
        if (!overlayIsNull() && !getOverlay().isEmpty()) 
            buf.append(getStringListArgs(" --overlay ", getOverlay()));
        if (StringUtils.isNotBlank(getPemPath())) {
            buf.append(" --pem-path ");
            buf.append(getPemPath());
        }
        if (isRocm()) buf.append(" --rocm");
        
        if (!scratchIsNull() && !getScratch().isEmpty()) 
            buf.append(getStringListArgs(" --scratch ", getScratch()));
        if (!securityIsNull() && !getSecurity().isEmpty()) 
            buf.append(getStringListArgs(" --security ", getSecurity()));
        
        if (isUserNs()) buf.append(" --userns");
        if (isUts()) buf.append(" --uts");
        if (StringUtils.isNotBlank(getWorkdir())) {
            buf.append(" --workdir ");
            buf.append(getWorkdir());
        }
        if (isWritable()) buf.append(" --writable");
        if (isWritableTmpfs()) buf.append(" --writable-tmpfs");
    }
    
    /* ---------------------------------------------------------------------- */
    /* getPairListArgs:                                                       */
    /* ---------------------------------------------------------------------- */
    /** Create the string of key=value pairs separated by new line characters.
     * 
     * @param values NON-EMPTY list of pair values, one per occurance
     * @return the string that contains all assignments
     */
    protected String getPairListArgs(List<Pair<String,String>> pairs)
    {
        // Get a buffer to accumulate the key/value pairs.
        final int capacity = 1024;
        StringBuilder buf = new StringBuilder(capacity);
        
        // Create a list of key=value assignment, each followed by a new line.
        for (var v : pairs) {
            buf.append(v.getLeft());
            buf.append("=");
            buf.append(v.getRight());
            buf.append("\n");
        }
        return buf.toString();
    }
    
    /* ---------------------------------------------------------------------- */
    /* getStringListArgs:                                                     */
    /* ---------------------------------------------------------------------- */
    /** Create the string of multiple occurrence arguments from a non-empty
     * list. 
     * 
     * @param arg the multiple occurrence argument padded with spaces on both sides 
     * @param values NON-EMPTY list of values, one per occurance
     * @return the string that contains all assignments
     */
    protected String getStringListArgs(String arg, List<String> values)
    {
        String s = "";
        for (var v : values) s += arg + v;
        return s;
    }
    
    /* ********************************************************************** */
    /*                               Accessors                                */
    /* ********************************************************************** */
    // List null checks.
    public boolean envIsNull()         {return env == null;}
    public boolean networkArgsIsNull() {return networkArgs == null;}
    public boolean noMountsIsNull()    {return noMounts == null;}
    public boolean overlayIsNull()     {return overlay == null;}
    public boolean scratchIsNull()     {return scratch == null;}
    public boolean securityIsNull()    {return security == null;}
    
    public String getCapabilities() {
        return capabilities;
    }
    public void setCapabilities(String capabilities) {
        this.capabilities = capabilities;
    }
    public String getBind() {
        return bind;
    }
    public void setBind(String bind) {
        this.bind = bind;
    }
    public boolean isCleanEnv() {
        return cleanEnv;
    }
    public void setCleanEnv(boolean cleanEnv) {
        this.cleanEnv = cleanEnv;
    }
    public boolean isContain() {
        return contain;
    }
    public void setContain(boolean contain) {
        this.contain = contain;
    }
    public boolean isContainAll() {
        return containAll;
    }
    public void setContainAll(boolean containAll) {
        this.containAll = containAll;
    }
    public boolean isDisableCache() {
        return disableCache;
    }
    public void setDisableCache(boolean disableCache) {
        this.disableCache = disableCache;
    }
    public String getDns() {
        return dns;
    }
    public void setDns(String dns) {
        this.dns = dns;
    }
    public String getDropCapabilities() {
        return dropCapabilities;
    }
    public void setDropCapabilities(String dropCapabilities) {
        this.dropCapabilities = dropCapabilities;
    }
    public List<Pair<String,String>> getEnv() {
        if (env == null) env = new ArrayList<Pair<String,String>>();
        return env;
    }
    public void setEnv(List<Pair<String,String>> env) {
        this.env = env;
    }
    public String getEnvFile() {
        return envFile;
    }
    public void setEnvFile(String envFile) {
        this.envFile = envFile;
    }
    public String getHome() {
        return home;
    }
    public void setHome(String home) {
        this.home = home;
    }
    public String getHostname() {
        return hostname;
    }
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
    public String getImage() {
        return image;
    }
    public void setImage(String image) {
        this.image = image;
    }
    public boolean isNet() {
        return net;
    }
    public void setNet(boolean net) {
        this.net = net;
    }
    public String getNetwork() {
        return network;
    }
    public void setNetwork(String network) {
        this.network = network;
    }
    public List<String> getNetworkArgs() {
        if (networkArgs == null) networkArgs = new ArrayList<String>();
        return networkArgs;
    }
    public void setNetworkArgs(List<String> networkArgs) {
        this.networkArgs = networkArgs;
    }
    public boolean isNoHome() {
        return noHome;
    }
    public void setNoHome(boolean noHome) {
        this.noHome = noHome;
    }
    public boolean isNoInit() {
        return noInit;
    }
    public void setNoInit(boolean noInit) {
        this.noInit = noInit;
    }
    public List<String> getNoMounts() {
        if (noMounts == null) noMounts = new ArrayList<String>();
        return noMounts;
    }
    public void setNoMounts(List<String> noMounts) {
        this.noMounts = noMounts;
    }
    public boolean isNoPrivs() {
        return noPrivs;
    }
    public void setNoPrivs(boolean noPrivs) {
        this.noPrivs = noPrivs;
    }
    public boolean isNoUMask() {
        return noUMask;
    }
    public void setNoUMask(boolean noUMask) {
        this.noUMask = noUMask;
    }
    public boolean isNoHTTPS() {
        return noHTTPS;
    }
    public void setNoHTTPS(boolean noHTTPS) {
        this.noHTTPS = noHTTPS;
    }
    public boolean isNv() {
        return nv;
    }
    public void setNv(boolean nv) {
        this.nv = nv;
    }
    public List<String> getOverlay() {
        if (overlay == null) overlay = new ArrayList<String>();
        return overlay;
    }
    public void setOverlay(List<String> overlay) {
        this.overlay = overlay;
    }
    public String getPemPath() {
        return pemPath;
    }
    public void setPemPath(String pemPath) {
        this.pemPath = pemPath;
    }
    public boolean isRocm() {
        return rocm;
    }
    public void setRocm(boolean rocm) {
        this.rocm = rocm;
    }
    public List<String> getScratch() {
        if (scratch == null) scratch = new ArrayList<String>();
        return scratch;
    }
    public void setScratch(List<String> scratch) {
        this.scratch = scratch;
    }
    public List<String> getSecurity() {
        if (security == null) security = new ArrayList<String>();
        return security;
    }
    public void setSecurity(List<String> security) {
        this.security = security;
    }
    public boolean isUserNs() {
        return userNs;
    }
    public void setUserNs(boolean userNs) {
        this.userNs = userNs;
    }
    public boolean isUts() {
        return uts;
    }
    public void setUts(boolean uts) {
        this.uts = uts;
    }
    public String getWorkdir() {
        return workdir;
    }
    public void setWorkdir(String workdir) {
        this.workdir = workdir;
    }
    public boolean isWritable() {
        return writable;
    }
    public void setWritable(boolean writable) {
        this.writable = writable;
    }
    public boolean isWritableTmpfs() {
        return writableTmpfs;
    }
    public void setWritableTmpfs(boolean writableTmpfs) {
        this.writableTmpfs = writableTmpfs;
    }
 }
