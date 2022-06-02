package edu.utexas.tacc.tapis.jobs.reader;

import java.time.Instant;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;

import edu.utexas.tacc.tapis.jobs.config.RuntimeParameters;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.JobEvent;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobStatusType;
import edu.utexas.tacc.tapis.jobs.queue.DeliveryResponse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManager.ExchangeUse;
import edu.utexas.tacc.tapis.jobs.queue.JobQueueManagerNames;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.NotificationsClient;
import edu.utexas.tacc.tapis.notifications.client.gen.model.Event;
import edu.utexas.tacc.tapis.shared.TapisConstants;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.ServiceContext;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

/** This class reads serialized JobEvents placed on the event queue by the 
 * api or worker processes, creates a Notification service event and posts
 * that event to Notifications.
 * 
 * @author rcardone
 */
public final class EventReader
 extends AbstractQueueReader
{
    /* ********************************************************************** */
    /*                               Constants                                */
    /* ********************************************************************** */
    // Tracing.
    private static final Logger _log = LoggerFactory.getLogger(EventReader.class);

    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // The tenant's alternate queue name.
    private final String _queueName;
    
    // Name of the exchange used by this queue.
    private final String _exchangeName;
    
    private String       _siteAdminTenantId;
    
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public EventReader(QueueReaderParameters parms) 
    {
        // Assign superclass field.
        super(parms);
        
        // Force use of the default binding key.
        _parms.bindingKey = JobQueueManagerNames.DEFAULT_BINDING_KEY;
        
        // Save the queue name in a field.
        _queueName = JobQueueManagerNames.getEventQueueName();
        
        // Save the exchange name;
        _exchangeName = JobQueueManagerNames.getEventExchangeName();
        
        // Print configuration.
        _log.info(getStartUpInfo(_queueName, _exchangeName));
    }

    /* ********************************************************************** */
    /*                             Public Methods                             */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) 
     throws JobException 
    {
        // Parse the command line parameters.
        QueueReaderParameters parms = null;
        try {parms = new QueueReaderParameters(args);}
          catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_START_ERROR", e.getMessage());
            _log.error(msg, e);
            throw e;
          }
        
        // Start the worker.
        EventReader reader = new EventReader(parms);
        reader.start();
    }

    /* ---------------------------------------------------------------------- */
    /* start:                                                                 */
    /* ---------------------------------------------------------------------- */
    /** Initialize the process and its threads. */
    public void start()
      throws JobException
    {
      // Announce our arrival.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STARTED", _parms.name, 
                                    _queueName, getBindingKey()));
      
      // Get our service tokens.
      initReaderEnv();
      
      // Start reading the queue.
      readQueue();
      
      // Announce our termination.
      if (_log.isInfoEnabled()) 
          _log.info(MsgUtils.getMsg("JOBS_READER_STOPPED", _parms.name, 
                                    _queueName, getBindingKey()));
    }

    /* ********************************************************************** */
    /*                            Protected Methods                           */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* process:                                                               */
    /* ---------------------------------------------------------------------- */
    /** Process a delivered message.
     * 
     * @param delivery the incoming message and its metadata
     * @return true if the message was successfully processed, false if the 
     *          message should be rejected and discarded without redelivery
     */
    @Override
    protected boolean process(DeliveryResponse delivery)
    {
        // Tracing
        if (_log.isDebugEnabled()) { 
            String msg = JobQueueManager.getInstance().dumpMessageInfo(
              delivery.consumerTag, delivery.envelope, delivery.properties, delivery.body);
            _log.debug(msg);
        }
        
        // The body should always be a UTF-8 json string.
        String body;
        try {body = new String(delivery.body, "UTF-8");}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("ALOE_BYTE_ARRAY_DECODE", new String(Hex.encodeHex(delivery.body)));
                _log.error(msg);
                return false;
            }
        
        // Decode the input.
        JobEvent jobEvent = null;
        try {jobEvent = TapisGsonUtils.getGson(true).fromJson(body, JobEvent.class);}
            catch (Exception e) {
                if (body.length() > JSON_DUMP_LEN) body = body.substring(0, JSON_DUMP_LEN - 1);
                String msg = MsgUtils.getMsg("ALOE_JSON_PARSE_ERROR", getName(), body, e.getMessage());
                _log.error(msg, e);
                return false;
            }
        
        // Make sure we got some message type.
        if (jobEvent.getEvent() == null) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INVALD_MSG_TYPE", "null", getName());
            _log.error(msg);
            return false;
        }
        
        // Populate a Notifications event.
        Event event = new Event();
        event.setSource(TapisConstants.SERVICE_NAME_JOBS);
        event.setType(makeNotifEventType(jobEvent.getEvent(), jobEvent.getEventDetail()));
        event.setSubject(jobEvent.getJobUuid());
        event.setSeriesId(jobEvent.getJobUuid());
        event.setData(jobEvent.getDescription());
        event.setTimestamp(Instant.now().toString());
// TODO:        event.setDeleteSubscriptionsMatchingSubject(isLastEvent(jobEvent.getEvent(), jobEvent.getEventDetail()));
        
        // Get a Notification's client.
        NotificationsClient client = null;
        try {client = JobUtils.getNotificationsClient(_siteAdminTenantId);} 
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_NOT_FOUND", "Notifications",
                                             _siteAdminTenantId, TapisConstants.SERVICE_NAME_JOBS);
                _log.error(msg, e);
                return false;
            }
        
        // Push the event to Notifications.
        try {client.postEvent(event);}
            catch (Exception e) {
                String msg = MsgUtils.getMsg("TAPIS_CLIENT_ERROR", "Notifications",
                                             _siteAdminTenantId, TapisConstants.SERVICE_NAME_JOBS);
                _log.error(msg, e);
                return false;
            }
        
        // Success.
        return true;
    }   

    /* ---------------------------------------------------------------------- */
    /* getName:                                                               */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getName() {return _parms.name;}

    /* ---------------------------------------------------------------------- */
    /* getExchangeType:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected BuiltinExchangeType getExchangeType() {return BuiltinExchangeType.FANOUT;}
    
    /* ---------------------------------------------------------------------- */
    /* getExchangeName:                                                       */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getExchangeName() {return _exchangeName;}
    
    /* ---------------------------------------------------------------------------- */
    /* getExchangeUse:                                                              */
    /* ---------------------------------------------------------------------------- */
    @Override
    protected ExchangeUse getExchangeUse() {return ExchangeUse.ALT;}
    
    /* ---------------------------------------------------------------------- */
    /* getQueueName:                                                          */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getQueueName() {return _queueName;}

    /* ---------------------------------------------------------------------- */
    /* getBindingKey:                                                         */
    /* ---------------------------------------------------------------------- */
    @Override
    protected String getBindingKey() {return _parms.bindingKey;}

    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* makeNotifEventType:                                                    */
    /* ---------------------------------------------------------------------- */
    /** Event type string are matched against Notification typeFilters in 
     * subscriptions. Subscription typeFilters that have this general format:
     * 
     *     service.category.eventDetail
     * 
     * Job subscription typeFilters always looks like this:
     * 
     *     jobs.<jobEventType>.*
     * 
     * Event strings generated by this method always have 3 components filled
     * in with no wildcards.
     * 
     * See JobSubscriptionResource for the subscription creation implementation.
     * 
     * @param eventType the well-defined Jobs event types
     * @param detail the particular event name
     * @return the 3 part event type string
     */
    private String makeNotifEventType(JobEventType eventType, String detail)
    {
        return JobUtils.makeNotifTypeToken(eventType, detail);
    }
    
    /* ---------------------------------------------------------------------- */
    /* isLastEvent:                                                           */
    /* ---------------------------------------------------------------------- */
    /** Determine if this is the last status message that a specific job will
     * ever receive.  If so, we can tell Notifications to delete all subscriptions
     * specifically targeting this job.
     * 
     * @param eventType the well-defined Jobs event types
     * @param detail the particular event name
     * @return true if this is the last event on this subject, false otherwise
     */
    private boolean isLastEvent(JobEventType eventType, String detail)
    {
        // Only status event signal that subscriptions can be immediately removed.
        if (eventType != JobEventType.JOB_NEW_STATUS) return false;
        
        // Determine if the new job status is a terminal state.
        try {
            var status = JobStatusType.valueOf(detail);
            if (status.isTerminal()) return true;
        } catch (Exception e) {/* this should not happen */}
        
        // Non-terminal statuses.
        return false;
    }
    
    /* ---------------------------------------------------------------------- */
    /* initReaderEnv:                                                         */
    /* ---------------------------------------------------------------------- */
    private void initReaderEnv()
     throws JobException
    {
        // Already initialized, but assigned for convenience.
        var parms = RuntimeParameters.getInstance();
        
        // Force runtime initialization of the tenant manager.  This creates the
        // singleton instance of the TenantManager that can then be accessed by
        // all subsequent application code--including filters--without reference
        // to the tenant service base url parameter.
        Map<String,Tenant> tenantMap = null;
        try {
            // The base url of the tenants service is a required input parameter.
            // We actually retrieve the tenant list from the tenant service now
            // to fail fast if we can't access the list.
            String url = parms.getTenantBaseUrl();
            tenantMap = TenantManager.getInstance(url).getTenants();
        } catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "TenantManager", e.getMessage());
            throw new JobException(msg, e);
        }
        if (!tenantMap.isEmpty()) {
            String msg = ("\n--- " + tenantMap.size() + " tenants retrieved:\n");
            for (String tenant : tenantMap.keySet()) msg += "  " + tenant + "\n";
            _log.info(msg);
        } else {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "TenantManager", "Empty tenant map.");
            throw new JobException(msg);
        }
        
        // Save the site's administrative tenant id.
        _siteAdminTenantId = TenantManager.getInstance().getSiteAdminTenantId(parms.getSiteId());
        _log.info("\n--- Admin tenant for site \"" + parms.getSiteId() + "\" is: " + _siteAdminTenantId + "\n");
        
        // ----- Service JWT Initialization
        ServiceContext serviceCxt = ServiceContext.getInstance();
        try {
                 serviceCxt.initServiceJWT(parms.getSiteId(), TapisConstants.SERVICE_NAME_JOBS, 
                                           parms.getServicePassword());
        }
        catch (Exception e) {
            String msg = MsgUtils.getMsg("JOBS_WORKER_INIT_ERROR", "ServiceContext", e.getMessage());
            throw new JobException(msg, e);
        }
        
        // Print site info.
        {
            var targetSites = serviceCxt.getServiceJWT().getTargetSites();
            int targetSiteCnt = targetSites != null ? targetSites.size() : 0;
            String msg = "\n--- " + targetSiteCnt + " target sites retrieved:\n";
            if (targetSites != null) {
                for (String site : targetSites) msg += "  " + site + "\n";
            }
            _log.info(msg);
        }
    }
    
}
