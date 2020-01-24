package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkSecretMetadata;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSecretMeta 
 extends RespAbstract
{
    public RespSecretMeta(SkSecretMetadata meta) {result = meta;}
    
    public SkSecretMetadata result;
}
