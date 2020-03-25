package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminDBCredential 
 extends SkAdminAbstractSecret
{
    public String tenant;
    public String user;
    public String dbservice;
    public String dbhost;
    public String dbname;
    public String secretName;
    public String secret;
    public String kubeSecretName;
}
