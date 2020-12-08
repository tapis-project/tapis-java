package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;

import java.time.Instant;

/*
 * Capability class representing a capability supported by a TSystem, such as what job schedulers the system supports,
 *   what software is on the system, the hardware on which the system is running, the type of OS the system is running,
 *   the version of the OS, container support, etc.
 * Each TSystem definition contains a list of capabilities supported by that system.
 * An Application or Job definition may specify required capabilities.
 * Used for determining eligible systems for running an application or job.
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 * Tenant + system + category + subcategory + name must be unique.
 *
 * NOTE: In the database a capability also includes tenant, system_id, created and updated.
 *       Currently tenant and system_id should be known in the context in which this class is used
 *         and the created, updated timestamps are not being used.
 */
public final class Capability
{
  public enum Category {SCHEDULER, OS, HARDWARE, SOFTWARE, JOB, CONTAINER, MISC, CUSTOM}
  public enum Datatype{STRING, INTEGER, BOOLEAN, NUMBER, TIMESTAMP}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  public static final String DEFAULT_VALUE = "";
  public static final String DEFAULT_SUBCATEGORY = "";
  public static final int DEFAULT_PRECEDENCE = 100;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  private final int seqId;           // Unique database sequence number
  private final int systemId;

  private final Category category; // Type or category of capability
  private final String subcategory;   // Name of the capability
  private final String name;   // Name of the capability
  private final Datatype datatype; // Datatype associated with the value
  private final int precedence;  // Precedence. Higher number has higher precedence.
  private final String value;  // Value or range of values

  private final Instant created; // UTC time for when record was created
  private final Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  // Constructor initializing all fields.
  public Capability(int id1, int systemid1, Category category1, String subcategory1, String name1,
                    Datatype datatype1, int precedence1, String value1, Instant created1, Instant updated1)
  {
    seqId = id1;
    systemId = systemid1;
    created = created1;
    updated = updated1;
    category = category1;
    subcategory = subcategory1;
    name = name1;
    datatype = datatype1;
    precedence = precedence1;
    value = value1;
  }

  // Constructor initializing minimal number of fields, useful for testing. Should not be persisted.
  public Capability(Category category1, String subcategory1, String name1, Datatype datatype1, int precedence1, String value1)
  {
    seqId = -1;
    systemId = -1;
    created = null;
    updated = null;
    category = category1;
    subcategory = subcategory1;
    name = name1;
    datatype = datatype1;
    precedence = precedence1;
    value = value1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public int getSeqId() { return seqId; }
  public int getSystemId() { return systemId; }
  public Instant getCreated() { return created; }
  public Instant getUpdated() { return updated; }
  public Category getCategory() { return category; }
  public String getSubCategory() { return subcategory; }
  public String getName() { return name; }
  public Datatype getDatatype() { return datatype; }
  public int getPrecedence() { return precedence; }
  public String getValue() { return value; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
