package edu.utexas.tacc.tapis.security.commands.model;

import java.util.List;

public final class SkAdminSecrets 
{
    public List<SkAdminDBCredential> dbcredential;
    public List<SkAdminJwtSigning>   jwtsigning;
    public List<SkAdminServicePwd>   servicepwd;
    public List<SkAdminUser>         user;
}
