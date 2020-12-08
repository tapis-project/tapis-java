package edu.utexas.tacc.tapis.systems.model;

import edu.utexas.tacc.tapis.shared.utils.TapisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/*
 * A queue that maps to a single HPC queue. Provides a uniform front end abstraction for an HPC queue.
 *   Also provides more features and flexibility than is typically provided by an HPC scheduler.
 *   Multiple logical queues may be defined for each HPC queue.
 *
 * This class is intended to represent an immutable object.
 * Please keep it immutable.
 *
 * system_id + name must be unique.
 *
 * NOTE: In the database a logical queue also includes system_id, created and updated.
 *       Currently system_id should be known in the context in which this class is used
 *         and the created, updated timestamps are not being used.
 */
public final class LogicalQueue
{

  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */

  public static final String DEFAULT_VALUE = "";
  public static final String DEFAULT_SUBCATEGORY = "";
  public static final int DEFAULT_PRECEDENCE = 100;

  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  // Logging
  private static final Logger _log = LoggerFactory.getLogger(LogicalQueue.class);

  private final int id;           // Unique database sequence number
  private final int systemid;

  private final String name;   // Name for the logical queue
  private final int maxJobs;
  private final int maxJobsPerUser;
  private final int maxNodeCount;
  private final int maxCoresPerNode;
  private final int maxMemoryMB;
  private final int maxMinutes;
  private final Instant created; // UTC time for when record was created
  private final Instant updated; // UTC time for when record was last updated

  /* ********************************************************************** */
  /*                           Constructors                                 */
  /* ********************************************************************** */
  public LogicalQueue(int id1, int systemid1, String name1, int maxJobs1, int maxJobsPerUser1, int maxNodeCount1,
                      int maxCoresPerNode1, int maxMemoryMB1, int maxMinutes1, Instant created1, Instant updated1)
  {
    id = id1;
    systemid = systemid1;
    created = created1;
    updated = updated1;
    name = name1;
    maxJobs = maxJobs1;
    maxJobsPerUser = maxJobsPerUser1;
    maxNodeCount = maxNodeCount1;
    maxCoresPerNode = maxCoresPerNode1;
    maxMemoryMB = maxMemoryMB1;
    maxMinutes = maxMinutes1;
  }

  public LogicalQueue(String name1, int maxJobs1, int maxJobsPerUser1, int maxNodeCount1, int maxCoresPerNode1,
                      int maxMemoryMB1, int maxMinutes1)
  {
    id = -1;
    systemid = -1;
    created = null;
    updated = null;
    name = name1;
    maxJobs = maxJobs1;
    maxJobsPerUser = maxJobsPerUser1;
    maxNodeCount = maxNodeCount1;
    maxCoresPerNode = maxCoresPerNode1;
    maxMemoryMB = maxMemoryMB1;
    maxMinutes = maxMinutes1;
  }

  /* ********************************************************************** */
  /*                               Accessors                                */
  /* ********************************************************************** */
  public String getName() { return name; }
  public int getMaxJobs() { return maxJobs; }
  public int getMaxJobsPerUser() { return maxJobsPerUser; }
  public int getMaxNodeCount() { return maxNodeCount; }
  public int getMaxCoresPerNode() { return maxCoresPerNode; }
  public int getMaxMemoryMB() { return maxMemoryMB; }
  public int getMaxMinutes() { return maxMinutes; }

  @Override
  public String toString() {return TapisUtils.toString(this);}
}
