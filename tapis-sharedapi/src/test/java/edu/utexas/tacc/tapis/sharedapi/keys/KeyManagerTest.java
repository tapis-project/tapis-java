package edu.utexas.tacc.tapis.sharedapi.keys;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;

import org.apache.commons.codec.binary.Hex;
import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;

public class KeyManagerTest 
{
    /* **************************************************************************** */
    /*                                   Constants                                  */
    /* **************************************************************************** */
    private static final String KEY_STORE_PASSWORD = "!akxK3CuHfqzI#97";
    private static final String KEY_ALIAS = "tapis-jwt";
    private static final String TEST_STORE_FILE_NAME  = ".TapisTestKeyStore.p12";
    
    /* **************************************************************************** */
    /*                                    Tests                                     */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* test1:                                                                       */
    /* ---------------------------------------------------------------------------- */
    /** 
     * 
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws CertificateException
     * @throws IOException
     * @throws UnrecoverableKeyException
     * @throws InterruptedException
     */
    @Test(enabled=true)
    public void test1() 
     throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, 
            UnrecoverableKeyException, InterruptedException 
    { 
        // Load a new or existing key store.
        KeyManager km = new KeyManager(null, TEST_STORE_FILE_NAME);
        km.load(KEY_STORE_PASSWORD);
        
        // Get the jwt private key.
        PrivateKey privk = getPrivateKey(km);
        System.out.println("Private key algorithm: " + privk.getAlgorithm());
        System.out.println("Private key format: " + privk.getFormat());
        byte[] privBytes = privk.getEncoded();
        int privLen = 0;
        if (privBytes != null) privLen = privBytes.length;
        System.out.println("Private key length: " + privLen + " bytes");
        String privString = Hex.encodeHexString(privBytes);
        System.out.println("Private key:\n" + privString);
        
        // Create the signing key pair.
        Certificate cert = km.getCertificate(KEY_ALIAS);
        PublicKey pubk = cert.getPublicKey();
        System.out.println("\nPublic key algorithm: " + pubk.getAlgorithm());
        System.out.println("Public key format: " + pubk.getFormat());
        System.out.println("Public key:\n" + pubk.toString());
    }
    
    /* **************************************************************************** */
    /*                               Private Methods                                */
    /* **************************************************************************** */
    /* ---------------------------------------------------------------------------- */
    /* getPrivateKey:                                                               */
    /* ---------------------------------------------------------------------------- */
    private PrivateKey getPrivateKey(KeyManager km) 
     throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, 
            IOException, InterruptedException, CertificateException
    {
        // See if the private key already exists.
        PrivateKey privk = km.getPrivateKey(KEY_ALIAS, KEY_STORE_PASSWORD);
        if (privk != null) return privk;
        
        // We need to create the key.
        createKey(km.getStorePath());
        
        // Reload the store.
        km.load(KEY_STORE_PASSWORD);
        
        // Retrieve the key.
        return km.getPrivateKey(KEY_ALIAS, KEY_STORE_PASSWORD);
    }
    
    /* ---------------------------------------------------------------------------- */
    /* createKey:                                                                   */
    /* ---------------------------------------------------------------------------- */
    private void createKey(String storePath) throws IOException, InterruptedException
    {
        // Create the shell command.  Note that there's no need for padding nor for
        // quoting multi-word arguments.
        ArrayList<String> cmdList = new ArrayList<>(16);
        cmdList.add("keytool");
        cmdList.add("-keystore");
        cmdList.add(storePath);
        cmdList.add("-genkeypair");
        cmdList.add("-keyalg");
        cmdList.add("RSA");
        cmdList.add("-alias");
        cmdList.add(KEY_ALIAS);
        cmdList.add("-keysize");
        cmdList.add("2048");
        cmdList.add("-storetype");
        cmdList.add("PKCS12");
        cmdList.add("-dname");
        cmdList.add("CN=Richard Cardone, OU=Texas Advanced Computing Center, O=University of Texas at Austin, L=Austin, ST=Texas, C=US");
        cmdList.add("-storepass");
        cmdList.add(KEY_STORE_PASSWORD);
        
        // Create the process builder.
        System.out.println("Calling keytool to create \"" + KEY_ALIAS + "\" key pair in " + storePath + ".");
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.redirectErrorStream(true);
        pb.inheritIO();
        Process process = pb.start();
        int rc = process.waitFor();
        Assert.assertEquals(rc, 0, "Key creation failed with return code " + rc + ".");
    }
}
