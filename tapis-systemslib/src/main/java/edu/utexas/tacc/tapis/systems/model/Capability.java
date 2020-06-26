package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Tenant + system + category + name must be unique.
 *
 * NOTE: In the database a capability also includes tenant, system_id, created and updated.
 *       Currently tenant and system_id should be known in the context in which this class is used
 *         and the created, updated timestamps are not being used.
 */
public final class Capability
{
  public enum Category {SCHEDULER, OS, HARDWARE, SOFTWARE, JOB, CONTAINER, MISC, CUSTOM}

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(Capability.class);

  // NOTE: In order to use jersey's SelectableEntityFilteringFeature fields cannot be final.
  private int id;           // Unique database sequence number
  private int systemid;

  private Category category; // Type or category of capability
  private String name;   // Name of the capability
  private String value;  // Value or range of values
  private Instant created; // UTC time for when record was created
  private Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  // Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
  public Capability() { }
  public Capability(int id1, int systemid1, Category category1, String name1, String value1, Instant created1, Instant updated1)
  {
    id = id1;
    systemid = systemid1;
    created = created1;
    updated = updated1;
    category = category1;
    name = name1;
    value = value1;
  }

  public Capability(Category category1, String name1, String value1)
  {
    category = category1;
    name = name1;
    value = value1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  // NOTE: Setters that are not public are in place in order to use jersey's SelectableEntityFilteringFeature.
  public Category getCategory() { return category; }
  void setCategory(Category c) { category = c; }
  public String getName() { return name; }
  void setName(String s) { name = s; }
  public String getValue() { return value; }
  void setValue(String s) { value = s; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
