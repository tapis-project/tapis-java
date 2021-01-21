package edu.utexas.tacc.tapis.jobs.queue;

class JobQueueManagerNames 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // ----- RabbitMQ naming constants.
  // The Jobs virtual host.
  public static final String JOBS_VHOST = "jobs";
    
  // The prefix to all queuing components for the jobs service.
  private static final String TAPIS_JOBQ_PREFIX = "tapis.jobq.";
  
  // The submission queue part that follows the tenant id and precedes the queue name.
  private static final String SUBMIT_PART = "submit.";
  private static final String DEFAULT_SUBMIT_QUEUE_SUFFIX = "DefaultQueue";
  private static final String DEFAULT_SUBMIT_QUEUE = TAPIS_JOBQ_PREFIX + SUBMIT_PART + DEFAULT_SUBMIT_QUEUE_SUFFIX;
  
  // Components of exchange names used for tenants.
  private static final String SUBMIT_EXCHANGE_SUFFIX = "submit.Exchange";
  private static final String SUBMIT_EXCHANGE_NAME = TAPIS_JOBQ_PREFIX + SUBMIT_EXCHANGE_SUFFIX;
  
  // Recovery exchange and queue name components.
  private static final String RECOVERY_EXCHANGE_SUFFIX = "recovery.Exchange";
  private static final String RECOVERY_EXCHANGE_NAME = TAPIS_JOBQ_PREFIX + RECOVERY_EXCHANGE_SUFFIX;
  private static final String RECOVERY_QUEUE_SUFFIX = "recovery.Queue";
  private static final String RECOVERY_QUEUE_NAME = TAPIS_JOBQ_PREFIX + RECOVERY_QUEUE_SUFFIX;
  
  // Components for command topic exchange and queue names.
  private static final String CMD_TOPIC_PREFIX = TAPIS_JOBQ_PREFIX + "cmd.";
  private static final String TOPIC_CMD_EXCHANGE_NAME = CMD_TOPIC_PREFIX + "Exchange";
  private static final String TOPIC_CMD_TOPIC_NAME    = CMD_TOPIC_PREFIX + "Topic";
  
  // All worker command routing and binding keys.  Use keys as is.
  private static final String TOPIC_CMD_ALL_WORKER_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker";
  private static final String TOPIC_CMD_ALL_WORKER_BINDING_KEY = CMD_TOPIC_PREFIX + "worker";
  
  // Specific worker command routing and binding keys.  Append worker uuid to the end of keys.
  private static final String TOPIC_CMD_WORKER_WID_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker.wid.";
  private static final String TOPIC_CMD_WORKER_WID_BINDING_KEY = CMD_TOPIC_PREFIX + "worker.wid.";
  
  // Specific job command routing and binding keys.  Append job uuid to the end of keys.
  private static final String TOPIC_CMD_WORKER_JID_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker.jid.";
  private static final String TOPIC_CMD_WORKER_JID_BINDING_KEY = CMD_TOPIC_PREFIX + "worker.jid.";

  /* ********************************************************************** */
  /*                              Public Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getDefaultSubmitExchangeName:                                          */
  /* ---------------------------------------------------------------------- */
  /** Create the default exchange name used to communicate with
   * the default queue.
   * 
   * @return the default exchange name
   */
  public static String getSubmitExchangeName()
  {
    return SUBMIT_EXCHANGE_NAME;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdExchangeName:                                                    */
  /* ---------------------------------------------------------------------- */
  /** Create the global exchange name used to communicate with
   * the tenant's command topic queues.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant command topic exchange name
   */
  public static String getCmdExchangeName()
  {
    return TOPIC_CMD_EXCHANGE_NAME;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getRecoveryExchangeName:                                               */
  /* ---------------------------------------------------------------------- */
  /** Create the global exchange name used to communicate with
   * the tenant's recovery queue.  This exchanges captures messages that
   * have either:
   * 
   *    - Been rejected (basic.reject or basic.nack) with requeue=false,
   *    - Have their TTL expires, or
   *    - Would have caused a queue length limit to be exceeded.
   * 
   * @return the tenant recovery exchange name
   */
  public static String getRecoveryExchangeName()
  {
    return RECOVERY_EXCHANGE_NAME;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdTopicName:                                                       */
  /* ---------------------------------------------------------------------- */
  /** Create the global topic queue name used to communicate job 
   * commands to workers.
   * 
   * @param tenantId the id of a specific tenant
   * @param workerName the name of the worker assigned on worker start up
   * @return the tenant command topic name
   */
  public static String getCmdTopicName(String workerName)
  {
    return TOPIC_CMD_TOPIC_NAME + "." + workerName;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdSpecificJobTopicName:                                            */
  /* ---------------------------------------------------------------------- */
  /** Create the topic queue name used to communicate job commands to a 
   * worker handling a specific job.
   * 
   * @param tenantId the id of a specific tenant
   * @param jobUuid the job uuid as a string
   * @return the tenant job-specific command topic name
   */
  public static String getCmdSpecificJobTopicName(String jobUuid)
  {
    return TOPIC_CMD_TOPIC_NAME + "." + jobUuid ;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getRecoveryQueueName:                                                  */
  /* ---------------------------------------------------------------------- */
  /** Create the global topic queue name used to access job recover
   * messages.
   * 
   * @return the tenant recovery queue name
   */
  public static String getRecoveryQueueName()
  {
    return RECOVERY_QUEUE_NAME;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdAllWorkerRoutingKey:                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets all job workers.
   * @return the routing key
   */
  public static String getCmdAllWorkerRoutingKey()
  {
    return TOPIC_CMD_ALL_WORKER_ROUTING_KEY;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdAllWorkerBindingKey:                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting all job workers.
   * @return the binding key
   */
  public static String getCmdAllWorkerBindingKey()
  {
    return TOPIC_CMD_ALL_WORKER_BINDING_KEY;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdSpecificWorkerRoutingKey:                                        */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets a specific job worker.
   * @param workerUUID the targeted worker's uuid
   * @return the routing key
   */
  public static String getCmdSpecificWorkerRoutingKey(String workerUUID)
  {
    return TOPIC_CMD_WORKER_WID_ROUTING_KEY + workerUUID;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdSpecificWorkerBindingKey:                                        */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting a specific job worker.
   * @param workerUUID the targeted worker's uuid
   * @return the binding key
   */
  public static String getCmdSpecificWorkerBindingKey(String workerUUID)
  {
    return TOPIC_CMD_WORKER_WID_BINDING_KEY + workerUUID + ".#";
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdSpecificJobRoutingKey:                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets the worker processing a specific job.
   *  @param jobUUID the targeted job's uuid
   * @return the routing key
   */
  public static String getCmdSpecificJobRoutingKey(String jobUUID)
  {
    return TOPIC_CMD_WORKER_JID_ROUTING_KEY + jobUUID;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdSpecificJobBindingKey:                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting the worker processing 
   * a specific job.
   * @param workerUUID the targeted job's uuid
   * @return the binding key
   */
  public static String getCmdSpecificJobBindingKey(String jobUUID)
  {
    return TOPIC_CMD_WORKER_JID_BINDING_KEY + jobUUID + ".#";
  }
  
  /* ---------------------------------------------------------------------- */
  /* getDefaultQueue:                                                       */
  /* ---------------------------------------------------------------------- */
  public static String getDefaultQueue()
  {
    return DEFAULT_SUBMIT_QUEUE;
  }
}
