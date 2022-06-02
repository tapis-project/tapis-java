package edu.utexas.tacc.tapis.jobs.api.utils;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryMethod;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryTarget;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

public class JobsApiUtils 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // The wildcard used in notifications subject filters.
    private static final String TYPE_FILTER_WILDCARD = "*";
    
    /* **************************************************************************** */
    /*                                Public Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* toHttpStatus:                                                                */
    /* ---------------------------------------------------------------------------- */
    public static Status toHttpStatus(Condition condition)
    {
        // Conditions are expected to have the exact same names as statuses.
        try {return Status.valueOf(condition.name());}
        catch (Exception e) {return Status.INTERNAL_SERVER_ERROR;}     
    }
    
    /* ---------------------------------------------------------------------------- */
    /* constructTenantURL:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Construct a path from the base url of the specified tenant and path.  We 
     * prevent double slashes from appearing between each of the components (url, path 
     * and pathSuffix) that comprise the final string.  We also guarentee that a 
     * single slash separates each of the components.
     * 
     * Exceptions are never thrown. 
     * 
     * The path with the optional suffix appended will be returned if the tenant's 
     * base url could not be found.  If the optional pathSuffix is provided, it will 
     * be appended to the constructed url with a preceding slash if necessary.
     * 
     * @param roleTenant the tenantId whose base url will be retrieved
     * @param path the path to append to the tenant's base url
     * @param pathSuffix optional suffix to append to the path
     * @return the tenant's base url with the path and suffix appended or just the 
     * 			path and suffix if the tenant is not found 
     */
    public static String constructTenantURL(String tenantId, String path, String pathSuffix)
    {
    	 // Append the optional suffix to the path to allow for early exit..
    	 if (!StringUtils.isBlank(pathSuffix)) {
    		 // Make sure there's exactly 1 slash between the path and suffix.
    		 if (path.endsWith("/") && pathSuffix.startsWith("/")) 
    			 pathSuffix = pathSuffix.substring(1);
    		 else if (!path.endsWith("/") && !pathSuffix.startsWith("/"))
    			 pathSuffix = "/" + pathSuffix;
    		 path += pathSuffix;
    	 }
    	 
    	 // Get the tenant object. TenantManager throws an exception if 
    	 // the tenant cannot be resolved.
    	 Tenant tenant;
    	 try {tenant = TenantManager.getInstance().getTenant(tenantId);}
    	 catch (Exception e) {return path;} // the error is already logged
    	 
    	 // Get the tenant record.
    	 String url = tenant.getBaseUrl();
    	 if (url == null) return path;
    	 
    	 // Construct the url with path separated by a slash.
		 if (url.endsWith("/") && path.startsWith("/")) 
			 path = path.substring(1);
		 else if (!url.endsWith("/") && !path.startsWith("/"))
			 path = "/" + path;
    	 return url + path;
    }
    
    /* ---------------------------------------------------------------------------- */
    /* postSubcriptionRequest:                                                      */
    /* ---------------------------------------------------------------------------- */
    /** Convert a subscribe request into a Notification ReqPostSubscription object
     * and pass that to Notifications to create a subscription.
     * 
     * @param reqSubscribe an incoming subscription request
     * @param user the owner of the subscription    
     * @param tenant the owner's tenant
     * @param jobUuid the target job's uuid
     * @return the new subscription's url returned by Notifications
     * @throws TapisClientException
     * @throws RuntimeException
     * @throws TapisException
     * @throws ExecutionException
     */
    public static String postSubcriptionRequest(ReqSubscribe reqSubscribe, String user,
                                                String tenant, String jobUuid) 
     throws TapisClientException, RuntimeException, TapisException, ExecutionException
    {
        // Populate the request object.  The subjectFilter is always the jobEventType.
        var notifReq = new ReqPostSubscription();
        notifReq.setDescription(reqSubscribe.getDescription());
        notifReq.setEnabled(reqSubscribe.getEnabled());
        notifReq.setTtlMinutes(reqSubscribe.getTTLMinutes());
        notifReq.setSubjectFilter(jobUuid);
        
        // Set the targets.
        var notifTargets = new ArrayList<DeliveryTarget>();
        String lastDeliveryMethod = null; // external to try block for error handling.
        try {
            // Convert each request target into a notification target.
            for (var reqTarget : reqSubscribe.getDeliveryTargets()) {
                var notifTarget = new DeliveryTarget();
                notifTarget.setDeliveryAddress(reqTarget.getDeliveryAddress());
                lastDeliveryMethod = reqTarget.getDeliveryMethod().name();
                var notifMethod = DeliveryMethod.valueOf(lastDeliveryMethod);
                notifTarget.setDeliveryMethod(notifMethod);
                notifTargets.add(notifTarget);
            } 
        } catch (Exception e) {
            // The only possible exception is the string to enum conversion.
            var msg = MsgUtils.getMsg("JOBS_UNKNOWN_ENUM", "DeliveryMethod", lastDeliveryMethod, jobUuid);
            throw new JobException(msg, e);
        }
        notifReq.setDeliveryTargets(notifTargets);
        
        // Fill in the required, non-payload request values. We leave the name and
        // tenant unassigned allowing Notifications to assign them.
        notifReq.setTypeFilter(getNotifTypeFilter(reqSubscribe.getEventCategoryFilter(), TYPE_FILTER_WILDCARD));
        notifReq.setOwner(user);
        
        // Send request to Notifications.
        var jobsImpl = JobsImpl.getInstance();
        return jobsImpl.postSubcription(notifReq, user, tenant);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* getNotifTypeFilter:                                                          */
    /* ---------------------------------------------------------------------------- */
    /** Create a notification type filter with the following format:
     * 
     *     service.category.eventDetail
     * 
     * which for job subscriptions always looks like this:
     * 
     *     jobs.<jobEventType>.*
     *     
     * See EventReaders.makeNotifEventType() for the schema to which all Job events
     * conform.     
     * 
     * @param jobEventType the 2nd component in a job subscription type filter
     * @param eventDetail specific event or the wildcard character
     * @return the 3 part type filter string
     */
    public static String getNotifTypeFilter(JobEventCategoryFilter filter, String eventDetail)
    {
        return JobUtils.makeNotifTypeFilter(filter, eventDetail);
    }

}
