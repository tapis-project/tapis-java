package edu.utexas.tacc.tapis.sharedq;

import com.rabbitmq.client.AMQP.BasicProperties;

class QueueManagerNames 
{
  /* ********************************************************************** */
  /*                               Constants                                */
  /* ********************************************************************** */
  // ----- RabbitMQ naming constants.
  // The prefix to all queuing components for the jobs service.
  public  static final String TAPIS_QUEUE_PREFIX = "tapis.";
  public  static final String ALL_TENANTS_NAME   = "AllTenants";
  
  // Alternate exchange and queue name components.
  protected static final String MULTI_TENANT_ALT_EXCHANGE_SUFFIX = ".alt.Exchange";
  private static final String MULTI_TENANT_ALT_QUEUE_SUFFIX = ".alt.Queue";
  
  // Dead letter exchange and queue name components.
  private static final String MULTI_TENANT_DEADLETTER_EXCHANGE_SUFFIX = ".dead.Exchange";
  private static final String MULTI_TENANT_DEADLETTER_QUEUE_SUFFIX = ".dead.Queue";
  
  // ----- RabbitMQ naming constants.
  // Used to build connection names.
  private static final String OUT_CONNECTION_SUFFIX = "-OutConnection";
  private static final String IN_CONNECTION_SUFFIX  = "-InConnection";
  
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
  public String getOutConnectionName(String instanceName)
  {
      return instanceName + OUT_CONNECTION_SUFFIX;
  }
  
  /* ---------------------------------------------------------------------- */
  /* getInConnectionName:                                                   */
  /* ---------------------------------------------------------------------- */
  public String getInConnectionName(String instanceName)
  {
      return instanceName + IN_CONNECTION_SUFFIX;
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
    return TAPIS_QUEUE_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_ALT_QUEUE_SUFFIX;
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
    return TAPIS_QUEUE_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_DEADLETTER_QUEUE_SUFFIX;
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
    return TAPIS_QUEUE_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_ALT_EXCHANGE_SUFFIX;
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
    return TAPIS_QUEUE_PREFIX + ALL_TENANTS_NAME + MULTI_TENANT_DEADLETTER_EXCHANGE_SUFFIX;
  }
  
}
