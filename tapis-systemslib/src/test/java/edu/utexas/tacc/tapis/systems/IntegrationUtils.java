package edu.utexas.tacc.tapis.systems;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.utexas.tacc.tapis.shared.utils.TapisGsonUtils;
import edu.utexas.tacc.tapis.systems.model.Capability;
import edu.utexas.tacc.tapis.systems.model.TSystem;
import edu.utexas.tacc.tapis.systems.model.TSystem.AccessMethod;
import edu.utexas.tacc.tapis.systems.model.TSystem.TransferMethod;

import java.util.ArrayList;
import java.util.List;

/*
 * Utilities and data for integration testing
 */
public final class IntegrationUtils
{
  // Test data
  public static final String tenantName = "dev";
  public static final String ownerUser = "owner1";
  public static final String ownerUser2 = "owner2";
  public static final String apiUser = "testApiUser";
  public static final String sysNamePrefix = "TestSys";
  public static final Gson gson =  TapisGsonUtils.getGson();
  public static final List<TransferMethod> txfrMethodsList = new ArrayList<>(List.of(TransferMethod.SFTP, TransferMethod.S3));
  public static final List<TransferMethod> txfrMethodsEmpty = new ArrayList<>();
  public static final String[] tags = {"value1", "value2", "a",
    "a long tag with spaces and numbers (1 3 2) and special characters [_ $ - & * % @ + = ! ^ ? < > , . ( ) { } / \\ | ]. Backslashes must be escaped."};
  public static final Object notes = TapisGsonUtils.getGson().fromJson("{\"project\": \"myproj1\", \"testdata\": \"abc1\"}", JsonObject.class);
  public static final JsonObject notesObj = (JsonObject) notes;
  public static final Protocol prot1 = new Protocol(AccessMethod.PKI_KEYS, txfrMethodsList, 22, false, "", 0);
  public static final Protocol prot2 = new Protocol(AccessMethod.PASSWORD, txfrMethodsList, 0, true, "localhost",2222);
  public static final String scrubbedJson = "{}";

  public static final Capability capA = new Capability(Capability.Category.SCHEDULER, "Type", "Slurm");
  public static final Capability capB = new Capability(Capability.Category.HARDWARE, "CoresPerNode", "4");
  public static final Capability capC = new Capability(Capability.Category.SOFTWARE, "OpenMP", "4.5");
  public static final Capability capD = new Capability(Capability.Category.CONTAINER, "Singularity", null);
  public static final List<Capability> capList = new ArrayList<>(List.of(capA, capB, capC, capD));

  /**
   * Create an array of TSystem objects in memory
   * Names will be of format TestSys_K_NNN where K is the key and NNN runs from 000 to 999
   * We need a key because maven runs the tests in parallel so each set of systems created by an integration
   *   test will need its own namespace.
   * @param n number of systems to create
   * @return array of TSystem objects
   */
  public static TSystem[] makeSystems(int n, String key)
  {
    TSystem[] systems = new TSystem[n];
    for (int i = 0; i < n; i++)
    {
      // Suffix which should be unique for each system within each integration test
      String suffix = key + "_" + String.format("%03d", i+1);
      String name = sysNamePrefix + "_" + suffix;
      // Constructor initializes all attributes except for JobCapabilities and Credential
      systems[i] = new TSystem(-1, tenantName, name, "description "+suffix, TSystem.SystemType.LINUX, ownerUser,
              "host"+suffix, true,"effUser"+suffix, prot1.getAccessMethod(), "bucket"+suffix, "/root"+suffix,
              prot1.getTransferMethods(), prot1.getPort(), prot1.isUseProxy(), prot1.getProxyHost(), prot1.getProxyPort(),
              false, "jobLocalWorkDir"+suffix, "jobLocalArchDir"+suffix, "jobRemoteArchSystem"+suffix,"jobRemoteArchDir1"+suffix,
              tags, notes, null, false, null, null);
      systems[i].setJobCapabilities(capList);
    }
    return systems;
  }
}
