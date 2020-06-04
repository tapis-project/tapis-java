package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.TSystem;

public final class RespSystem extends RespAbstract
{
  /**
   * Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
   */
  public RespSystem() { }

  public RespSystem(TSystem result) { this.result = result; }
  public TSystem result;
}
