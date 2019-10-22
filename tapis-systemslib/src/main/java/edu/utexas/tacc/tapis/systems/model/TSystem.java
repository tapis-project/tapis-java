package edu.utexas.tacc.tapis.systems.model;

import java.time.Instant;

import io.swagger.v3.oas.annotations.media.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * Tapis System representing a server or collection of servers exposed through a
 * single host name or ip address. Each system is associated with a specific tenant.
 * Tenant + name must be unique
 * Name of the system must be URI safe, see RFC 3986.
 *   Allowed characters: Alphanumeric  [0-9a-zA-Z] and special characters [-._~].
 * Each system has an owner, effective acccess user, access protocol, transfer protocol
 *   and flag indicating if it is currently available.
 */
public final class TSystem
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  public static final String DEFAULT_OWNER = "${apiUserId}";
  public static final boolean DEFAULT_AVAILABLE_ = true;
  public static final String DEFAULT_ROOTDIR = "/";
  public static final String DEFAULT_JOBINPUTDIR = "/input";
  public static final String DEFAULT_JOBOUTPUTDIR = "/output";
  public static final String DEFAULT_WORKDIR = "/data";
  public static final String DEFAULT_SCRATCHDIR = "/scratch";
  public static final String DEFAULT_EFFECTIVEUSERID = "${apiUserId}";

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(TSystem.class);

  private long id;         // Unique database sequence number
  private String tenant;   // Name of the tenant for which the system is defined
  private String name;     // Name of the system
  private String description;
  private String owner;
  private String host;
  private boolean available;
  private String bucketName;
  private String rootDir;
  private String jobInputDir;
  private String jobOutputDir;
  private String workDir;
  private String scratchDir;
  private String accessCredential;
  private Protocol protocol;
  private String effectiveUserId;
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                               Constructors                             */
  /* ********************************************************************** */
  public TSystem(long id1, String tenant1, String name1, String description1,
                 String owner1, String host1, boolean available1, String bucketName1,
                 String rootDir1, String jobInputDir1, String jobOutputDir1, String workDir1, String scratchDir1,
                 String effectiveUserId1, Protocol protocol1, String accessCredential1,
                 Instant created1, Instant updated1)
  {
    id = id1;
    tenant = tenant1;
    name = name1;
    description = description1;
    owner = owner1;
    host = host1;
    available = available1;
    bucketName = bucketName1;
    rootDir = rootDir1;
    jobInputDir = jobInputDir1;
    jobOutputDir = jobOutputDir1;
    workDir = workDir1;
    scratchDir = scratchDir1;
    effectiveUserId = effectiveUserId1;
    protocol = protocol1;
    accessCredential = accessCredential1;
    created = created1;
    updated = updated1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public long getId() { return id; }

  public String getTenant() { return tenant; }

  public String getName() { return name; }

  public String getDescription() { return description; }
  public void setDescription(String description) { this.description = description; }

  public String getOwner() { return owner; }

  public String getHost() { return host; }

  public boolean isAvailable() { return available; }
  public void setAvailable(boolean available) { this.available = available; }

  public String getBucketName() { return bucketName; }

  public String getRootDir() { return rootDir; }

  public String getJobInputDir() { return jobInputDir; }

  public String getJobOutputDir() { return jobOutputDir; }

  public String getWorkDir() { return workDir; }

  public String getScratchDir() { return scratchDir; }

  public String getEffectiveUserId() { return effectiveUserId; }

  public Protocol getProtocol() { return protocol; }

  public String getAccessCredential() { return accessCredential; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }
}
