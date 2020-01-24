package edu.utexas.tacc.tapis.security.api.responses;

import java.util.List;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespVersions 
 extends RespAbstract
{
    public RespVersions(List<Integer> versions) {result = versions;}
    
    public List<Integer> result;
}
