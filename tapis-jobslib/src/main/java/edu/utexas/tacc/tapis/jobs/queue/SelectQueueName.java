package edu.utexas.tacc.tapis.jobs.queue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.jobs.dao.JobQueuesDao;
import edu.utexas.tacc.tapis.jobs.exceptions.JobQueueFilterException;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.JobQueue;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shareddb.TapisDBUtils;

public final class SelectQueueName 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(SelectQueueName.class);
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* select:                                                                      */
    /* ---------------------------------------------------------------------------- */
    public String select(Job job)
    {
        // Populate substitution values.
        //
        // These values can only be class Boolean, Byte, Short, Integer, Long, 
        // Float, Double, and String; any other values will cause an exception.
        // Property names cannot be null or the empty string.
        Map<String, Object> properties = new HashMap<>();
        
        // Used by system to retrieve queue set and available for use in filters.
        properties.put("name", job.getName());
        properties.put("owner", job.getOwner());
        properties.put("tenant", job.getTenant());
        
        properties.put("type", job.getType().name());
        properties.put("exec_class", job.getExecClass().name());
        
        properties.put("created", job.getCreated().toString());
        properties.put("uuid", job.getUuid());
        
        properties.put("appId", job.getAppId());
        properties.put("appVersion", job.getAppVersion());
        
        properties.put("archiveOnAppError", job.isArchiveOnAppError());
        properties.put("dynamicExecSystem", job.isDynamicExecSystem());
        
        properties.put("execSystemId", job.getExecSystemId());
        properties.put("execSystemExecDir", job.getExecSystemExecDir());
        properties.put("execSystemInputDir", job.getExecSystemInputDir());
        properties.put("execSystemOutputDir", job.getExecSystemOutputDir());
        properties.put("execSystemLogicalQueue", job.getExecSystemLogicalQueue());
        
        properties.put("archiveSystemId", job.getArchiveSystemId());
        properties.put("archiveSystemDir", job.getArchiveSystemDir());
        
        properties.put("dtnSystemId", job.getDtnSystemId());
        properties.put("dtnMountSourcePath", job.getDtnMountSourcePath());
        properties.put("dtnMountPoint", job.getDtnMountPoint());
        
        properties.put("nodeCount", job.getNodeCount());
        properties.put("coresPerNode", job.getCoresPerNode());
        properties.put("memoryMB", job.getMemoryMB());
        properties.put("maxMinutes", job.getMaxMinutes());
        
        properties.put("tapisQueue", job.getTapisQueue());
        properties.put("createdby", job.getCreatedby());
        properties.put("createdbyTenant", job.getCreatedbyTenant());
        
        // When tags is not null or empty, assign the tags key a string value
        // of the form:  ('item1', 'item2', ...).
        // This allows filter clauses to written like:  'mytag' IN tags 
        if (job.getTags() != null)
           properties.put("tags", TapisDBUtils.makeSqlList(
                           job.getTags().stream().collect(Collectors.toList())));        
        
        // Evaluate each of this tenant's queues in priority order.
        // Note the single atomic access to the queue mapping; see
        // QueueManager.doRefreshQueueInfo() for a concurrency discussion.
        String selectedQueueName = null;
        List<JobQueue> queues = getQueues();
        for (JobQueue queue : queues) {
            if (runFilter(queue, properties)) {
                selectedQueueName = queue.getName();
                break;
            }
        }
          
        // Make sure we select some queue.
        if (selectedQueueName == null) {
            String defaultQueue = QueueManager.getDefaultQueue();
            _log.error(MsgUtils.getMsg("JOBS_QUEUE_FILTER_NONE", job.getTenant(), job.getName(), defaultQueue));
            
            // Select the default queue.
            selectedQueueName = defaultQueue;
        }
        
        return selectedQueueName;
    }

    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* runFilter:                                                                   */
    /* ---------------------------------------------------------------------------- */
    /** Given a job queue and a map of key/value pair, substitute the values
     * in for their keys in the queue's filter and evaluate the filter.  True
     * is only returned if the filter's boolean expression evaluates to true.
     * Evaluation exceptions cause false to be returned.
     * 
     * @param jobQueue the queue whose filter is being evaluated
     * @param properties the substitution values used to evaluate the filter
     * @return true if the filter evaluates to true, false otherwise
     */
    private boolean runFilter(JobQueue jobQueue, Map<String, Object> properties)
    {
        // Evaluate the filter field using the properties field values.
        boolean matched = false;
        try {matched = SelectorFilter.match(jobQueue.getFilter(), properties);}
          catch (JobQueueFilterException e) {
            String msg = MsgUtils.getMsg("JOBS_QUEUE_FILTER_EVAL_ERROR", 
                                         jobQueue.getName() + " filter failed: " + e.getMessage()); 
            _log.error(msg, e);
        }
        return matched;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getQueues:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private List<JobQueue> getQueues()
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
        
        // An error occurred. 
        return Collections.emptyList();
    }
    
}
