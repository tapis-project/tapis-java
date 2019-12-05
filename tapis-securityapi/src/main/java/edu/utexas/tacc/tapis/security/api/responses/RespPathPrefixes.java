package edu.utexas.tacc.tapis.security.api.responses;

import edu.utexas.tacc.tapis.security.authz.permissions.PermissionTransformer.Transformation;
import edu.utexas.tacc.tapis.sharedapi.responses.RespAbstract;

public final class RespPathPrefixes 
 extends RespAbstract
{
    public RespPathPrefixes(Transformation[] array){transformations = array;}
    
    public Transformation[] transformations;
}
