package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkSecretList;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSecretList
 extends RespAbstract
{
    public RespSecretList(SkSecretList list) {result = list;}
    
    public SkSecretList result;
}
