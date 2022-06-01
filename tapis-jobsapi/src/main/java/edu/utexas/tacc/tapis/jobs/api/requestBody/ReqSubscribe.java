package edu.utexas.tacc.tapis.jobs.api.requestBody;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.jobs.model.enumerations.JobEventType;
import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;

public class ReqSubscribe 
 implements IReqBody
{
    // Informational values.
    private String  description;
    private Boolean enabled = Boolean.TRUE;
    
    // Search and delivery values.
    private Integer      ttlMinutes;
    private JobEventType eventType;
    private List<NotifDeliveryTarget> deliveryTargets = new ArrayList<NotifDeliveryTarget>();
    
    @Override
    public String validate() {
        // Success.
        return null; // json schema validation is sufficient
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
    public JobEventType getEventType() {
        return eventType;
    }
    public void setEventType(JobEventType eventType) {
        this.eventType = eventType;
    }
    public List<NotifDeliveryTarget> getDeliveryTargets() {
        return deliveryTargets;
    }
    public void setDeliveryTargets(List<NotifDeliveryTarget> deliveryTargets) {
        this.deliveryTargets = deliveryTargets;
    }
}
