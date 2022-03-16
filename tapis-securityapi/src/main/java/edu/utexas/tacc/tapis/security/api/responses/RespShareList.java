package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkShareList;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespShareList
 extends RespAbstract
{
    public RespShareList(SkShareList list) {result = list;}
    
    public SkShareList result;
}
