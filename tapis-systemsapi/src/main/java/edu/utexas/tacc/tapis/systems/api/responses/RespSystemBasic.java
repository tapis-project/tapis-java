package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.SystemBasic;

public final class RespSystemBasic extends RespAbstract
{
  public SystemBasic result;

  // Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
  public RespSystemBasic() { }

  public RespSystemBasic(SystemBasic result) { this.result = result; }
}
