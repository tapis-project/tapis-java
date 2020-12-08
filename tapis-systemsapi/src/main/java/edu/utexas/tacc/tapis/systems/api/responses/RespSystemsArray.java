package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

public final class RespSystemsArray extends RespAbstract
{
  // Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
  public RespSystemsArray() { }

  public RespSystemsArray(List<TSystem> result) { this.result = result; }
  public List<TSystem> result;
}
