package edu.utexas.tacc.tapis.meta;

import edu.utexas.tacc.tapis.client.shared.exceptions.TapisClientException;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWT;
import edu.utexas.tacc.tapis.sharedapi.security.ServiceJWTParms;

public class ServiceJWTRunner {
  public static void main(String[] args) {
    ServiceJWTParms parms = new ServiceJWTParms();
    parms.setServiceName("meta");
    parms.setTenant("master");
    parms.setTokensBaseUrl("https://dev.develop.tapis.io");
  
    String servicePassword = "awzlENwICM03MeGcn7p8CDsmoTxsEfVDFd7Fp+f20pA=";
    try {
      ServiceJWT serviceJWT = new ServiceJWT(parms,servicePassword);
      System.out.println(serviceJWT.getAccessJWT());
    } catch (TapisException | TapisClientException e) {
      e.printStackTrace();
    }
  }
}
