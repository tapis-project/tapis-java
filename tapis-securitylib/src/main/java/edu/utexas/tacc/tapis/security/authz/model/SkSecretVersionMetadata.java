package edu.utexas.tacc.tapis.security.authz.model;

import java.util.ArrayList;
import java.util.List;

public final class SkSecretVersionMetadata 
{
    public String  created_time;
    public int     current_version;
    public int     max_versions;
    public int     oldest_version;
    public String  updated_time;
    
    // Initialize the list to be non-null.
    public List<SkSecretVersion> versions = new ArrayList<>();
}