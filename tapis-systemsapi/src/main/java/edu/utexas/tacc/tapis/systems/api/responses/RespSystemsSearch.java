package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespSearch;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultMetadata;
import edu.utexas.tacc.tapis.sharedapi.responses.results.ResultSearch;
import edu.utexas.tacc.tapis.systems.model.TSystem;

import java.util.List;

/*
  Results from a retrieval of TSystem resources.
 */
public final class RespSystemsSearch extends RespSearch
{

  // NOTE: Having this attribute here seems necessary although not clear why since it appears to be unused.
  //       Without it the returned json has java object references listed in the result.search list.
  public List<TSystem> results;

  // Zero arg constructor needed to use jersey's SelectableEntityFilteringFeature
  public RespSystemsSearch() { }

  public RespSystemsSearch(List<TSystem> tmpResults, int limit, String sortBy, int skip, String startAfter, int totalCount)
  {
    result = new ResultSearch();
    result.search = tmpResults;
    ResultMetadata tmpMeta = new ResultMetadata();
    tmpMeta.recordCount = tmpResults.size();
    tmpMeta.recordLimit = limit;
    tmpMeta.recordsSkipped = skip;
    tmpMeta.sortBy = sortBy;
    tmpMeta.startAfter = startAfter;
    tmpMeta.totalCount = totalCount;
    result.metadata = tmpMeta;
  }
}