package edu.utexas.tacc.tapis.security.authz.model;

import java.util.ArrayList;
import java.util.List;

public final class SkSecretList 
{
    // The actual path used in the list.
    public String secretPath;
    
    // Initialize the list to be non-null.
    public List<String> keys = new ArrayList<>();
}