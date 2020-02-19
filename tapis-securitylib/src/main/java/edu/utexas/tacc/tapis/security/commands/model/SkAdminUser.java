package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminUser 
 extends SkAdminAbstractSecret
{
    public String tenant;
    public String user;
    public String secretName;
    public String key;
    public String value;
    public String kubeSecretName;
}
