package edu.utexas.tacc.tapis.sharedapi.security;

import io.jsonwebtoken.Jwt;

import java.security.Principal;

/**
 *
 */
public class AuthenticatedUser implements Principal {

    private final String username;
    private final String tenantId;
    private final String roles;
    private final Jwt jwt;



    public AuthenticatedUser(String username, String tenantId, String roles, Jwt jwt) {
        this.username = username;
        this.tenantId = tenantId;
        this.roles = roles;
        this.jwt = jwt;
    }

    @Override
    public String getName() {
        return this.username;
    }

    public String getRoles() {
        return roles;
    }

    public String getUsername() {
        return username;
    }

    public String getTenantId() {
        return tenantId;
    }

    public Jwt getJwt() {
        return jwt;
    }
}

