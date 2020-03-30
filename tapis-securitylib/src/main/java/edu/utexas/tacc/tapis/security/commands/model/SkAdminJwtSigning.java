package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminJwtSigning 
 extends SkAdminAbstractSecret
{
    public String tenant;
    public String user;
    public String secretName;
    public String privateKey;
    public String publicKey;
    public String kubeSecretName;
}
