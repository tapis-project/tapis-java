package edu.utexas.tacc.tapis.jobs.model;

import java.util.ArrayList;
import java.util.List;

import edu.utexas.tacc.tapis.shared.model.NotifDeliveryTarget;

public class JobSubscription 
{
    // Basic identity fields.
    private String  description;
    private boolean enabled;
    
    // Search and delivery values.
    private int     ttlMinutes;
    private String  typeFilter;
    private String  subjectFilter;
    private List<NotifDeliveryTarget> deliveryTargets = new ArrayList<NotifDeliveryTarget>();
    
    // Constructors.
    public JobSubscription() {}
    public JobSubscription(edu.utexas.tacc.tapis.apps.client.gen.model.AppSubscription appSub)
    {
        description   = appSub.getDescription();
        enabled       = appSub.getEnabled() == null ? true : appSub.getEnabled();
        ttlMinutes    = appSub.getTtlMinutes() == null ? 0 : appSub.getTtlMinutes();
        typeFilter    = appSub.getTypeFilter();
        subjectFilter = appSub.getSubjectFilter();
        if (appSub.getDeliveryTargets() != null)
            for (var appTarget : appSub.getDeliveryTargets()) {
                var target = new NotifDeliveryTarget(appTarget);
                deliveryTargets.add(target);
            }
    }
    
    // Accessors
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    public boolean isEnabled() {
        return enabled;
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    public int getTtlMinutes() {
        return ttlMinutes;
    }
    public void setTtlMinutes(int ttlMinutes) {
        this.ttlMinutes = ttlMinutes;
    }
    public String getTypeFilter() {
        return typeFilter;
    }
    public void setTypeFilter(String typeFilter) {
        this.typeFilter = typeFilter;
    }
    public String getSubjectFilter() {
        return subjectFilter;
    }
    public void setSubjectFilter(String subjectFilter) {
        this.subjectFilter = subjectFilter;
    }
    public List<NotifDeliveryTarget> getDeliveryTargets() {
        return deliveryTargets;
    }
    public void setDeliveryTargets(List<NotifDeliveryTarget> deliveryTargets) {
        this.deliveryTargets = deliveryTargets;
    }
}
