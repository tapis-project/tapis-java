package edu.utexas.tacc.tapis.security.commands.model;

public final class SkAdminServicePwd
 extends SkAdminAbstractSecret
{
    public String tenant;
    public String service;
    public String secretName;
    public String password;
    public String kubeSecretName;
}
