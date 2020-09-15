package edu.utexas.tacc.tapis.systems.api.requests;

import edu.utexas.tacc.tapis.systems.model.Capability;

import java.util.List;

/*
 * Class representing system attributes that can be updated when updating a system that was created by
 * importing an SGCI resource definition.
 * sgciResourceId is required, all others are optional.
 */
public final class ReqUpdateSGCISystem
{
  public String sgciResourceId; // ID of SGCI resource - required
  public String description; // Description of the system
  public boolean enabled; // Indicates if systems is currently enabled
  public String effectiveUserId; // User to use when accessing system, may be static or dynamic
  public List<Capability> jobCapabilities; // List of job related capabilities supported by the system
  public String[] tags;       // List of arbitrary tags as strings
  public Object notes;      // Simple metadata as json
}
