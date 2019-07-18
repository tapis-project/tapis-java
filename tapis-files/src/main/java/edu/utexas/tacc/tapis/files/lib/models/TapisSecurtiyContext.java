package edu.utexas.tacc.tapis.files.lib.models;

import java.security.Principal;

import javax.ws.rs.core.SecurityContext;

public class TapisSecurtiyContext implements SecurityContext {

    private AuthenticatedUser user;

    public TapisSecurtiyContext(AuthenticatedUser user) {
        this.user = user;
    }

    @Override
    public Principal getUserPrincipal() {return this.user;}

    @Override
    public boolean isUserInRole(String s) {
        // TODO: implement this?
        return true;
    }

    @Override
    public boolean isSecure() {return true;}

    @Override
    public String getAuthenticationScheme() {
        return SecurityContext.CLIENT_CERT_AUTH;
    }

}
