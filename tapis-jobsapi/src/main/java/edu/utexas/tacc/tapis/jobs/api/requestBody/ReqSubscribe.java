package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;
import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;
import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget.DeliveryMethod;

public class ReqSubscribe 
 implements IReqBody
{
    // Default ttl is 1 week and the maximum is 4 weeks.
    public static final Integer DEFAULT_TTL_MINUTES = 10080; // 60*24*7
    public static final Integer MAX_TTL_MINUTES = DEFAULT_TTL_MINUTES * 4;
    
    // Informational values.
    private String  description;
    private Boolean enabled = Boolean.TRUE;
    
    // Search and delivery values.
    private Integer                   ttlMinutes;
    private JobEventCategoryFilter    eventCategoryFilter;
    private List<NotifDeliveryTarget> deliveryTargets = new ArrayList<NotifDeliveryTarget>();
    
    // Constructors.
    public ReqSubscribe() {}
    public ReqSubscribe(edu.utexas.tacc.tapis.apps.client.gen.model.ReqSubscribe appReq)
     throws JobException
    {
        // Marshal the app request to a job request object.  
        // Exceptions can be thrown during some conversions.
        try {
            description = appReq.getDescription();
            if (appReq.getEnabled() != null) enabled = appReq.getEnabled();
            ttlMinutes = appReq.getTtlMinutes();
            var appFilter = appReq.getJobEventCategoryFilter();
            if (appFilter != null) eventCategoryFilter = JobEventCategoryFilter.valueOf(appFilter.name());
            if (appReq.getDeliveryTargets() != null) 
                for (var appTarget : appReq.getDeliveryTargets()) { 
                    var target = new NotifDeliveryTarget();
                    target.setDeliveryAddress(appTarget.getDeliveryAddress());
                    var method = DeliveryMethod.valueOf(appTarget.getDeliveryMethod().name());
                    deliveryTargets.add(target);
                }
        } catch (Exception e) {
            var msg = MsgUtils.getMsg("JOBS_INITIALIZATION_ERROR", e.getMessage());
            throw new JobException(msg);
        }
        
        // Now validate this object's contents and assign ttl default if needed.
        String msg = validate();
        if (msg != null) throw new JobException(msg);
    }
    
    @Override
    public String validate() {
        // Even though the schema defines these fields as required, we doublecheck
        // here to support code paths that don't apply the schema.  We also assign
        // the ttl default if necessary.
        if (eventCategoryFilter == null) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "eventCategoryFilter");
        if (deliveryTargets == null || deliveryTargets.isEmpty())
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTargets");
        
        // Check each delivery target.
        for (var target : deliveryTargets) {
            if (StringUtils.isBlank(target.getDeliveryAddress()))
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTarget.deliveryAddress");
            if (target.getDeliveryMethod() == null)
                return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTarget.deliveryMethod");
        }
        
        // Assign ttl if necessary.
        if (ttlMinutes == null || ttlMinutes < 1) ttlMinutes = DEFAULT_TTL_MINUTES; 
        
        // Success.
        return null; 
    }
    
    // Accessors.
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public Boolean getEnabled() {
        return enabled;
    }
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }
    public Integer getTTLMinutes() {
        return ttlMinutes;
    }
    public void setTTLMinutes(Integer ttlMinutes) {
        this.ttlMinutes = ttlMinutes;
    }
    public JobEventCategoryFilter getEventCategoryFilter() {
        return eventCategoryFilter;
    }
    public void setEventCategoryFilter(JobEventCategoryFilter eventCategoryFilter) {
        this.eventCategoryFilter = eventCategoryFilter;
    }
    public List<NotifDeliveryTarget> getDeliveryTargets() {
        return deliveryTargets;
    }
    public void setDeliveryTargets(List<NotifDeliveryTarget> deliveryTargets) {
        this.deliveryTargets = deliveryTargets;
    }
}
