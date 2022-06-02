package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventCategoryFilter;
import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;

public class ReqSubscribe 
 implements IReqBody
{
    // Default ttl is 1 week.
    public static final Integer DEFAULT_TTL_MINUTES = 10080; // 60*24*7
    
    // Informational values.
    private String  description;
    private Boolean enabled = Boolean.TRUE;
    
    // Search and delivery values.
    private Integer                   ttlMinutes;
    private JobEventCategoryFilter    eventCategoryFilter;
    private List<NotifDeliveryTarget> deliveryTargets = new ArrayList<NotifDeliveryTarget>();
    
    @Override
    public String validate() {
        // Even though schema sets these fields as required, we doublecheck
        // here to support code paths that don't apply the schema.
        if (eventCategoryFilter == null) 
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "eventCategoryFilter");
        if (deliveryTargets == null || deliveryTargets.isEmpty())
            return MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "validate", "deliveryTargets");
        
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
