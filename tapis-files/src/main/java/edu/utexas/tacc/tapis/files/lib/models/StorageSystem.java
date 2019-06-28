package edu.utexas.tacc.tapis.files.lib.models;

import edu.utexas.tacc.tapis.shared.exceptions.TapisException;

import java.util.HashSet;
import java.util.Set;

public class StorageSystem {
    private Set<String> ALLOWED_PROTOCOLS = new HashSet<>();
    private String host;
    private Long port;
    private String baseDir;
    private String name;
    private String description;
    private String id;
    private String protocol;

    public StorageSystem() {
        ALLOWED_PROTOCOLS.add("S3");
        ALLOWED_PROTOCOLS.add("SSH");
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol)  throws TapisException{
        if (!(this.ALLOWED_PROTOCOLS.contains(protocol))) {
            throw new TapisException("Invalid Protocol");
        }
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Long getPort() {
        return port;
    }

    public void setPort(Long port) {
        this.port = port;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
