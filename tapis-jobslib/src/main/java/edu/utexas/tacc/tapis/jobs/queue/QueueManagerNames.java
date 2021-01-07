package edu.utexas.tacc.tapis.jobs.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;

class QueueManagerNames 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // Tracing.
  private static final Logger _log = LoggerFactory.getLogger(QueueManagerNames.class);
    
  // ----- RabbitMQ naming constants.
  // The prefix to all queuing components for the jobs service.
  public  static final String TAPIS_JOBQ_PREFIX = "tapis.jobq.";
  public  static final String ALL_TENANTS_NAME = "AllTenants";
  
  // The submission queue part that follows the tenant id and precedes the queue name.
  public  static final String TENANT_SUBMIT_PART = "submit.";
  private static final String DEFAULT_TENANT_QUEUE = "DefaultQueue";
  public  static final String TENANT_DEFAULT_QUEUE_SUFFIX = TENANT_SUBMIT_PART + DEFAULT_TENANT_QUEUE;
  public  static final String DEFAULT_TAPIS_QUEUE = TAPIS_JOBQ_PREFIX + "tapis" + "." + TENANT_DEFAULT_QUEUE_SUFFIX;
  
  // Used to build connection names.
  private static final String OUT_CONNECTION_SUFFIX = "-OutConnection";
  private static final String IN_CONNECTION_SUFFIX  = "-InConnection";
  
  // Components of exchange names used for tenants.
  private static final String TENANT_SUBMIT_EXCHANGE_SUFFIX = ".submit.Exchange";
  
  // Components for event topic exchange and queue names.
  public  static final String TENANT_EVENT_PART = "event.";
  private static final String TENANT_EVENT_EXCHANGE_SUFFIX = ".event.Exchange";
  private static final String TENANT_EVENT_TOPIC_SUFFIX    = ".event.Topic";
  private static final String EVENT_TOPIC_PREFIX = TAPIS_JOBQ_PREFIX + "event.";
  
  // Components for command topic exchange and queue names.
  public  static final String TENANT_CMD_PART = "cmd.";
  private static final String TENANT_CMD_EXCHANGE_SUFFIX = ".cmd.Exchange";
  private static final String TENANT_CMD_TOPIC_SUFFIX    = ".cmd.Topic";
  private static final String CMD_TOPIC_PREFIX = TAPIS_JOBQ_PREFIX + "cmd.";
  
  // Alternate exchange and queue name components.
  protected static final String MULTI_TENANT_ALT_EXCHANGE_SUFFIX = ".alt.Exchange";
  private static final String MULTI_TENANT_ALT_QUEUE_SUFFIX = ".alt.Queue";
  
  // Dead letter exchange and queue name components.
  private static final String MULTI_TENANT_DEADLETTER_EXCHANGE_SUFFIX = ".dead.Exchange";
  private static final String MULTI_TENANT_DEADLETTER_QUEUE_SUFFIX = ".dead.Queue";
  
  // Recovery exchange and queue name components.
  private static final String TENANT_RECOVERY_EXCHANGE_SUFFIX = ".recovery.Exchange";
  private static final String TENANT_RECOVERY_QUEUE_SUFFIX = ".recovery.Queue";
  
  // All subscriber event routing and binding keys.  Use keys as is.
  private static final String TOPIC_EVENT_ALL_SUBSCRIBER_ROUTING_KEY = EVENT_TOPIC_PREFIX + "subscriber";
  private static final String TOPIC_EVENT_ALL_SUBSCRIBER_BINDING_KEY = EVENT_TOPIC_PREFIX + "subscriber.#";
  
  // Specific correlation id event routing and binding keys.  Append correlation id to the end of keys. 
  private static final String TOPIC_EVENT_SUBSCRIBER_CID_ROUTING_KEY = EVENT_TOPIC_PREFIX + "subscriber.cid.";
  private static final String TOPIC_EVENT_SUBSCRIBER_CID_BINDING_KEY = EVENT_TOPIC_PREFIX + "subscriber.cid.";
  
  // Specific job event routing and binding keys.  Append job uuid to the end of keys.
  private static final String TOPIC_EVENT_SUBSCRIBER_JID_ROUTING_KEY = EVENT_TOPIC_PREFIX + "subscriber.jid.";
  private static final String TOPIC_EVENT_SUBSCRIBER_JID_BINDING_KEY = EVENT_TOPIC_PREFIX + "subscriber.jid.";
  
  // All worker command routing and binding keys.  Use keys as is.
  private static final String TOPIC_CMD_ALL_WORKER_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker";
  private static final String TOPIC_CMD_ALL_WORKER_BINDING_KEY = CMD_TOPIC_PREFIX + "worker";
  
  // Specific worker command routing and binding keys.  Append worker uuid to the end of keys.
  private static final String TOPIC_CMD_WORKER_WID_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker.wid.";
  private static final String TOPIC_CMD_WORKER_WID_BINDING_KEY = CMD_TOPIC_PREFIX + "worker.wid.";
  
  // Specific job command routing and binding keys.  Append job uuid to the end of keys.
  private static final String TOPIC_CMD_WORKER_JID_ROUTING_KEY = CMD_TOPIC_PREFIX + "worker.jid.";
  private static final String TOPIC_CMD_WORKER_JID_BINDING_KEY = CMD_TOPIC_PREFIX + "worker.jid.";

  // ----- RabbitMQ pre-configured properties objects.
  public static final BasicProperties PERSISTENT_JSON =
                        new BasicProperties("application/json",
                                            null,
                                            null,
                                            2,
                                            0, null, null, null,
                                            null, null, null, null,
                                            null, null);
  
  public static final BasicProperties PERSISTENT_TEXT =
                        new BasicProperties("text/plain",
                                            null,
                                            null,
                                            2,
                                            0, null, null, null,
                                            null, null, null, null,
                                            null, null);
 
  /* ********************************************************************** */
  /*                              Public Methods                            */
  /* ********************************************************************** */
  /* ---------------------------------------------------------------------- */
  /* getOutConnectionName:                                                  */
  /* ---------------------------------------------------------------------- */
  public String getOutConnectionName()
  {
      return RuntimeParameters.getInstance().getInstanceName() + OUT_CONNECTION_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getInConnectionName:                                                   */
  /* ---------------------------------------------------------------------- */
  public String getInConnectionName()
  {
      return RuntimeParameters.getInstance().getInstanceName() + IN_CONNECTION_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantSubmitExchangeName:                                           */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific exchange name used to communicate with
   * tenant queues.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant exchange name
   */
  public String getTenantSubmitExchangeName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_SUBMIT_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantEventExchangeName:                                            */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific exchange name used to communicate with
   * the tenant's event topic queues.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant event topic exchange name
   */
  public String getTenantEventExchangeName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_EVENT_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantCmdExchangeName:                                              */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific exchange name used to communicate with
   * the tenant's command topic queues.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant command topic exchange name
   */
  public String getTenantCmdExchangeName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_CMD_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAllTenantAltExchangeName:                                           */
  /* ---------------------------------------------------------------------- */
  /** Create the multi-tenant exchange name used to communicate with
   * the tenant's alternate exchange queue.  This exchange captures
   * message that would otherwise be unrouteable.
   * 
   * @return the tenant alternate exchange name
   */
  public String getAllTenantAltExchangeName()
  {
    return TAPIS_JOBQ_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_ALT_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAllTenantDeadLetterExchangeName:                                    */
  /* ---------------------------------------------------------------------- */
  /** Create the multi-tenant exchange name used to communicate with
   * the tenant's dead letter queue.  This exchanges captures messages that
   * have either:
   * 
   *    - Been rejected (basic.reject or basic.nack) with requeue=false,
   *    - Have their TTL expires, or
   *    - Would have caused a queue length limit to be exceeded.
   * 
   * @return the tenant dead letter exchange name
   */
  public String getAllTenantDeadLetterExchangeName()
  {
    return TAPIS_JOBQ_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_DEADLETTER_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantRecoveryExchangeName:                                         */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific exchange name used to communicate with
   * the tenant's recovery queue.  This exchanges captures messages that
   * have either:
   * 
   *    - Been rejected (basic.reject or basic.nack) with requeue=false,
   *    - Have their TTL expires, or
   *    - Would have caused a queue length limit to be exceeded.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant recovery exchange name
   */
  public String getTenantRecoveryExchangeName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_RECOVERY_EXCHANGE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantEventTopicName:                                               */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific topic queue name used to communicate job 
   * events between workers and other interested parties.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant event topic name
   */
  public String getTenantEventTopicName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_EVENT_TOPIC_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantCmdTopicName:                                                 */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific topic queue name used to communicate job 
   * commands to workers.
   * 
   * @param tenantId the id of a specific tenant
   * @param workerName the name of the worker assigned on worker start up
   * @return the tenant command topic name
   */
  public String getTenantCmdTopicName(String tenantId, String workerName)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_CMD_TOPIC_SUFFIX + "." + workerName;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getTenantCmdSpecificJobTopicName:                                      */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific topic queue name used to communicate job 
   * commands to workers.
   * 
   * @param tenantId the id of a specific tenant
   * @param jobUuid the job uuid as a string
   * @return the tenant job-specific command topic name
   */
  public String getTenantCmdSpecificJobTopicName(String tenantId, String jobUuid)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_CMD_TOPIC_SUFFIX + "." + jobUuid ;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAllTenantAltQueueName:                                              */
  /* ---------------------------------------------------------------------- */
  /** Create the multi-tenant queue name used to access unrouteable messages.
   * 
   * @return the tenant alternate route queue name
   */
  public String getAllTenantAltQueueName()
  {
    return TAPIS_JOBQ_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_ALT_QUEUE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getAllTenantDeadLetterQueueName:                                       */
  /* ---------------------------------------------------------------------- */
  /** Create the multi-tenant topic queue name used to access dead letter
   * messages.
   * 
   * @return the tenant dead letter queue name
   */
  public String getAllTenantDeadLetterQueueName()
  {
    return TAPIS_JOBQ_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_DEADLETTER_QUEUE_SUFFIX;
  }
  /* ---------------------------------------------------------------------- */
  /* getTenantRecoveryQueueName:                                            */
  /* ---------------------------------------------------------------------- */
  /** Create the tenant-specific topic queue name used to access job recover
   * messages.
   * 
   * @param tenantId the id of a specific tenant
   * @return the tenant recovery queue name
   */
  public String getTenantRecoveryQueueName(String tenantId)
  {
    return TAPIS_JOBQ_PREFIX + tenantId + TENANT_RECOVERY_QUEUE_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventAllSubscriberRoutingKey:                                       */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets all event subscribers.
   * @return the routing key
   */
  public String getEventAllSubscriberRoutingKey()
  {
    return TOPIC_EVENT_ALL_SUBSCRIBER_ROUTING_KEY;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventAllWorkerBindingKey:                                           */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting all event subscribers.
   * @return the binding key
   */
  public String getEventAllSubscriberBindingKey()
  {
    return TOPIC_EVENT_ALL_SUBSCRIBER_BINDING_KEY;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventSpecificSubscriberRoutingKey:                                  */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets a specific subscriber using a 
   * previously established correlation id.
   * @param correlationId the correlation id of interested subscribers
   * @return the routing key
   */
  public String getEventSpecificSubscriberRoutingKey(String correlationId)
  {
    return TOPIC_EVENT_SUBSCRIBER_CID_ROUTING_KEY + correlationId;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventSpecificSubscriberBindingKey:                                  */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting a specific job worker.
   * @param correlationId the correlation id of interest
   * @return the binding key
   */
  public String getEventSpecificSubscriberBindingKey(String correlationId)
  {
    return TOPIC_EVENT_SUBSCRIBER_CID_BINDING_KEY + correlationId + ".#";
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventSpecificJobRoutingKey:                                         */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets subscribers interested in a specific job.
   *  @param jobUUID the targeted job's uuid
   * @return the routing key
   */
  public String getEventSpecificJobRoutingKey(String jobUUID)
  {
    return TOPIC_EVENT_SUBSCRIBER_JID_ROUTING_KEY + jobUUID;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getEventSpecificJobBindingKey:                                         */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting a specific job.
   * @param jobUUID the targeted job's uuid
   * @return the binding key
   */
  public String getEventSpecificJobBindingKey(String jobUUID)
  {
    return TOPIC_EVENT_SUBSCRIBER_JID_BINDING_KEY + jobUUID + ".#";
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdAllWorkerRoutingKey:                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the routing key that targets all job workers.
   * @return the routing key
   */
  public String getCmdAllWorkerRoutingKey()
  {
    return TOPIC_CMD_ALL_WORKER_ROUTING_KEY;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getCmdAllWorkerBindingKey:                                             */
  /* ---------------------------------------------------------------------- */
  /** Get the binding key that accepts messages targeting all job workers.
   * @return the binding key
   */
  public String getCmdAllWorkerBindingKey()
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
  public String getCmdSpecificWorkerRoutingKey(String workerUUID)
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
  public String getCmdSpecificWorkerBindingKey(String workerUUID)
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
  public String getCmdSpecificJobRoutingKey(String jobUUID)
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
  public String getCmdSpecificJobBindingKey(String jobUUID)
  {
    return TOPIC_CMD_WORKER_JID_BINDING_KEY + jobUUID + ".#";
  }
  
  /* ---------------------------------------------------------------------- */
  /* getDefaultQueue:                                                       */
  /* ---------------------------------------------------------------------- */
  public static String getDefaultQueue()
  {
    return DEFAULT_TAPIS_QUEUE;
  }
}
