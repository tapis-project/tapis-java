package edu.utexas.tacc.tapis.jobs.api.utils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.jobs.api.requestBody.ReqSubscribe;
import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.impl.JobsImpl;
import edu.utexas.tacc.tapis.jobs.model.Job;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.jobs.utils.JobUtils;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryMethod;
import edu.utexas.tacc.tapis.notifications.client.gen.model.DeliveryTarget;
import edu.utexas.tacc.tapis.notifications.client.gen.model.ReqPostSubscription;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisImplException.Condition;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.security.TenantManager;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.tenants.client.gen.model.Tenant;

public class JobsApiUtils 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    // The wildcard used in notifications subject filters.
    public static final String TYPE_FILTER_WILDCARD = "*";
    
    // Create a TypeToken to be used by gson for processing of LinkedTreeMap objects.
    private static final Type linkedTreeMapType = new TypeToken<LinkedTreeMap<Object,Object>>(){}.getType();

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
    /* postSubscriptionRequest:                                                     */
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
    public static String postSubscriptionRequest(ReqSubscribe reqSubscribe, String user,
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
        return jobsImpl.postSubscription(notifReq, user, tenant);
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
    
    /* ---------------------------------------------------------------------------- */
    /* convertInputObjectToString:                                                  */
    /* ---------------------------------------------------------------------------- */
    /** This specialized method allows user input defined with Java type Object and
     * json schema type object to be processed and validated.  It accepts the known 
     * concrete types of String and LinkedTreeMap, all other types throw an exception.  
     * The exceptions thrown by this method will abort any in progress API request. 
     * 
     * @param obj a json input object as some expected type
     * @return a validated json string
     * @throws TapisImplException when object to string conversion fails
     */
    public static String convertInputObjectToString(Object obj)
     throws TapisImplException
    {
        // Caller should customize null case if default isn't applicable.
        if (obj == null) return Job.EMPTY_JSON;
        
        // Input objects originating from apps or systems are strings.
        if (obj instanceof String) {
            String objStr = (String) obj;
            if (StringUtils.isBlank(objStr)) {
                String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "convertInputObjectToString", "obj");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            
            // Make sure we have a well-formed json object.
            try {TapisGsonUtils.getGson().fromJson(objStr, JsonObject.class);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_JSON_PARSE_ERROR", "convertInputObjectToString",
                                                 objStr, e.getMessage());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            
            // Return the serialized json object.
            return objStr;
        }
        
        // Input objects originating from a job interface are gson LinkedTreeMaps.
        if (obj instanceof LinkedTreeMap<?,?>) {
            // Convert obj to a string.
            String objStr = null;
            try {objStr = TapisGsonUtils.getGson().toJson(obj, linkedTreeMapType);}
                catch (Exception e) {
                    String msg = MsgUtils.getMsg("TAPIS_JSON_SERIALIZATION_ERROR", 
                                                 obj.getClass().getSimpleName(), e.getMessage());
                    throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
                }
            
            // Make sure we got something.
            if (StringUtils.isBlank(objStr)) {
                String msg = MsgUtils.getMsg("TAPIS_JSON_SERIALIZATION_ERROR", 
                                             obj.getClass().getSimpleName(), "null");
                throw new TapisImplException(msg, Status.BAD_REQUEST.getStatusCode());
            }
            
            // Return the serialized json object.
            return objStr;
        } else {
            // Not a gson LinkedTreeMap.
            String msg = MsgUtils.getMsg("TAPIS_JSON_UNEXPECTED_OBJECT_TYPE", "obj", 
                                         obj.getClass().getSimpleName(), "LinkedTreeMap");
            throw new TapisImplException(msg, Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }
    }
}
