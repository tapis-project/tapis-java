package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminDBCredential 
 extends SkAdminAbstractSecret
{
    public String service;
    public String dbhost;
    public String dbname;
    public String dbuser;
    public String secretName;
    public String secret;
    public String kubeSecretName;
}
