package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.systems.model.TSystem.AuthnMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemType;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.Instant;

/*
 * Tapis SystemBasic - minimal number of attributes from a TSystem.
 *
 * Tenant + name must be unique
 *
 * Make defensive copies as needed on get/set to keep this class as immutable as possible.
 */
public final class SystemBasic
{
  // ************************************************************************
  // *********************** Constants **************************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Fields *****************************************
  // ************************************************************************

  private int seqId;           // Unique database sequence number
  private String tenant;     // Name of the tenant for which the system is defined
  private String name;       // Name of the system
  private TSystem.SystemType systemType; // Type of system, e.g. LINUX, OBJECT_STORE
  private String owner;      // User who owns the system and has full privileges
  private String host;       // Host name or IP address
  private TSystem.AuthnMethod defaultAuthnMethod; // How access authorization is handled by default
  private boolean canExec; // Indicates if system will be used to execute jobs
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  // ************************************************************************
  // *********************** Constructors ***********************************
  // ************************************************************************

  /**
   * Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
   * NOTE: Adding a default constructor changes jOOQ behavior such that when Record.into() uses the default mapper
   *       the column names and POJO attribute names must match (with convention an_attr -> anAttr).
   */
  public SystemBasic() { }

  /**
   * Constructor using only required attributes.
   */
  public SystemBasic(String name1, SystemType systemType1, String host1, AuthnMethod defaultAuthnMethod1, boolean canExec1)
  {
    name = name1;
    systemType = systemType1;
    host = host1;
    defaultAuthnMethod = defaultAuthnMethod1;
    canExec = canExec1;
  }

  /**
   * Construct using a TSystem
   */
  public SystemBasic(TSystem tSystem)
  {
    if (tSystem != null)
    {
      seqId = tSystem.getSeqId();
      tenant = tSystem.getTenant();
      name = tSystem.getId();
      systemType = tSystem.getSystemType();
      host = tSystem.getHost();
      defaultAuthnMethod = tSystem.getDefaultAuthnMethod();
      canExec = tSystem.getCanExec();
      created = tSystem.getCreated();
      updated = tSystem.getUpdated();
    }
  }

//  /**
//   * Copy constructor. Returns a deep copy of a SystemBasic object.
//   * Make defensive copies as needed.
//   */
//  public SystemBasic(SystemBasic t)
//  {
//    if (t==null) throw new IllegalArgumentException(LibUtils.getMsg("SYSLIB_NULL_INPUT"));
//    id = t.getId();
//    created = t.getCreated();
//    updated = t.getUpdated();
//    tenant = t.getTenant();
//    name = t.getName();
//    systemType = t.getSystemType();
//    owner = t.getOwner();
//    host = t.getHost();
//    defaultAuthnMethod = t.getDefaultAuthnMethod();
//    canExec = t.getCanExec();
//  }

  // ************************************************************************
  // *********************** Public methods *********************************
  // ************************************************************************

  // ************************************************************************
  // *********************** Accessors **************************************
  // ************************************************************************

  public int getSeqId() { return seqId; }

  @Schema(type = "string")
  public Instant getCreated() { return created; }

  @Schema(type = "string")
  public Instant getUpdated() { return updated; }

  public String getTenant() { return tenant; }
  public SystemBasic setTenant(String s) { tenant = s; return this; }

  public String getName() { return name; }
  public SystemBasic setName(String s) { name = s; return this; }

  public SystemType getSystemType() { return systemType; }

  public String getOwner() { return owner; }
  public SystemBasic setOwner(String s) { owner = s;  return this;}

  public String getHost() { return host; }
  public SystemBasic setHost(String s) { host = s; return this; }

  public AuthnMethod getDefaultAuthnMethod() { return defaultAuthnMethod; }
  public SystemBasic setDefaultAuthnMethod(AuthnMethod a) { defaultAuthnMethod = a; return this; }

  public boolean getCanExec() { return canExec; }
}
