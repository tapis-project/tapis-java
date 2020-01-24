package edu.utexas.tacc.tapis.security.authz.model;

import java.util.HashMap;
import java.util.Map;

public final class SkSecret 
{
    // Fields
    public Map<String,String> secretMap = new HashMap<String,String>();
    public SkSecretMetadata metadata;
}
