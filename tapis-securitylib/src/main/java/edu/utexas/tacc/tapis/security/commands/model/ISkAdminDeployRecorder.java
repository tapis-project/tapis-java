package edu.utexas.tacc.tapis.security.commands.model;

public interface ISkAdminDeployRecorder 
{
    void addDeployRecord(String kubeSecretName, String kubeKey, String base64Secret);
}
