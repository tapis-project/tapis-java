package edu.utexas.tacc.tapis.jobs.queue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobQueuesDao;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.uuid.TapisUUID;
import edu.utexas.tacc.tapis.shared.uuid.UUIDType;

/** This class provides an in-memory cache of the job_queues table
 * that defines all queues associated with all tenants.  The queues are listed
 * in high to low priority ordering.
 *  
 * @author rcardone
 */
public class SubmitQueues 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(SubmitQueues.class);
  
  /* ********************************************************************** */
  /*                                 Fields                                 */
  /* ********************************************************************** */
  /** The prioritized list of queues defined for all tenants.  This list is 
   * initialized statically and can be reloaded afterwards on demand.
   */
  private static List<JobQueue> _submitQueues = loadQueues();
  
  /* ********************************************************************** */
  /*                             Public Methods                             */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getQueues:                                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the prioritized list of queue, which should not be empty and
   * is always non-null.
   * 
   * @return the non-null list of prioritized tenant queues
   */
  public static List<JobQueue> getQueues(){return _submitQueues;}
  
  /* ---------------------------------------------------------------------- */
  /* reloadQueues:                                                          */
  /* ---------------------------------------------------------------------- */
  /** Allow the tenant/queue mapping be loaded on demand.  This is not
   * expected to happen often.  Concurrency is allowed because of JVM 
   * guarantees of atomic address writes. 
   * @throws TapisException 
   */
  public static void reloadQueues() throws TapisException
  {
    _submitQueues = loadQueues();
  }
  
  /* ********************************************************************** */
  /*                             Private Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* loadQueues:                                                            */
  /* ---------------------------------------------------------------------- */
  private static List<JobQueue> loadQueues()
  {
      // Dump the table.
      try {
          // Get the list of all queues in descending priority order.
          var queueDao = new JobQueuesDao();
          return queueDao.getJobQueuesByPriorityDesc();
      }
      catch (Exception e) {
          String msg = MsgUtils.getMsg("JOBS_QUEUE_FAILED_ALL_QUERY", e.getMessage());
          _log.error(msg, e);
      }
      
      // --- Error case ---
      // We were unable to load the table, so define the default queue only.
      var q = new JobQueue();
      q.setId(1);
      q.setName(JobQueueManagerNames.getDefaultQueue());
      q.setFilter("tenant is not null");
      q.setPriority(1);
      q.setUuid(new TapisUUID(UUIDType.JOB_QUEUE).toString());
      var now = Instant.now();
      q.setCreated(now);
      q.setLastUpdated(now);
      
      // Add the queue to a list.
      var list = new ArrayList<JobQueue>(1);
      list.add(q);
      return list;
  }
}
