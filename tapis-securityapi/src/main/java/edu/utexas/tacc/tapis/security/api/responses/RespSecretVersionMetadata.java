package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.model.SkSecretVersionMetadata;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespSecretVersionMetadata
 extends RespAbstract
{
    public RespSecretVersionMetadata(SkSecretVersionMetadata meta) {result = meta;}
    
    public SkSecretVersionMetadata result;
}
