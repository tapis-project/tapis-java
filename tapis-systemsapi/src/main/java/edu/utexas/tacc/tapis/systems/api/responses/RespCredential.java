package edu.utexas.tacc.tapis.systems.api.responses;

import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;
import edu.utexas.tacc.tapis.systems.model.Credential;

public final class RespCredential extends RespAbstract
{
    public RespCredential(Credential result) { this.result = result;}
    
    public Credential result;
}
