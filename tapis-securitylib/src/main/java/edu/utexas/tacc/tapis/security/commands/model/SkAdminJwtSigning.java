package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminJwtSigning 
 extends SkAdminAbstractSecret
{
    public String tenant;
    public String secretName;
    public String secret;
    public String kubeSecretName;
}
