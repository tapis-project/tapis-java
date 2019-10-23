package edu.utexas.tacc.tapis.sharedapi.utils;

import java.io.Console;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.utexas.tacc.tapis.shared.i18n.MsgUtils;
import edu.utexas.tacc.tapis.sharedapi.keys.KeyManager;

/** This class creates a public/private key pair and stores them in the default
 * keystore as defined in KeyManager.  If the key pair exists, the user will be
 * prompted to replace it.  The key store will be created if it doesn't exist.
 * 
 * @author rcardone
 */
public class CreateKey 
{
    /* ********************************************************************** */
    /*                                   Constants                            */
    /* ********************************************************************* */
    // Local logger.
    private static final Logger _log = LoggerFactory.getLogger(CreateKey.class);
    
    // Generated key size.
    private static final String KEY_SIZE = "2048";
    
    /* ********************************************************************** */
    /*                                 Fields                                 */
    /* ********************************************************************** */
    // Command line parameters.
    private final CreateKeyParameters _parms;
        
    /* ********************************************************************** */
    /*                              Constructors                              */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* constructor:                                                           */
    /* ---------------------------------------------------------------------- */
    public CreateKey(CreateKeyParameters parms)
    {
        // Parameters cannot be null.
        if (parms == null) {
          String msg = MsgUtils.getMsg("TAPIS_NULL_PARAMETER", "CreateKey", "parms");
          _log.error(msg);
          throw new IllegalArgumentException(msg);
        }
        _parms = parms;
    }
    
    /* ********************************************************************** */
    /*                                Public Methods                          */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* main:                                                                  */
    /* ---------------------------------------------------------------------- */
    public static void main(String[] args) throws Exception 
    {
        CreateKeyParameters parms = new CreateKeyParameters(args);
        CreateKey ck = new CreateKey(parms);
        ck.exec();
    }

    /* ---------------------------------------------------------------------- */
    /* exec:                                                                  */
    /* ---------------------------------------------------------------------- */
    /** Main execution method. 
     * @throws KeyStoreException 
     * @throws IOException 
     * @throws CertificateException 
     * @throws NoSuchAlgorithmException 
     * @throws UnrecoverableKeyException 
     * @throws RuntimeException 
     * @throws InterruptedException 
     * @throws SignatureException 
     * @throws NoSuchProviderException 
     * @throws InvalidKeyException */
    public void exec() 
     throws KeyStoreException, NoSuchAlgorithmException, CertificateException, 
            IOException, UnrecoverableKeyException, InterruptedException, RuntimeException, 
            InvalidKeyException, NoSuchProviderException, SignatureException
    {
        // Load a new or existing key store.
        KeyManager km = new KeyManager(null,_parms.keyStorefile);
        km.load(_parms.password);
        
        // See if the key already exists.
        PrivateKey privk = km.getPrivateKey(_parms.alias, _parms.password);
        if (privk != null) {
            if (!confirmKeyReplace()) {
                _log.info(MsgUtils.getMsg("TAPIS_TERMINATE_NO_CHANGES", CreateKey.class.getSimpleName()));
                return;
            }
        }
        
        // We need to create the key.
        createKey(km.getStorePath());
        
        // Reload the store.
        km.load(_parms.password);
        
        // Verify and output key information.
        retrieveKeys(km);
       
     }
    
    /* ********************************************************************** */
    /*                             Private Methods                            */
    /* ********************************************************************** */
    /* ---------------------------------------------------------------------- */
    /* createKey:                                                             */
    /* ---------------------------------------------------------------------- */
    private void createKey(String storePath) 
      throws IOException, InterruptedException, RuntimeException
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
        cmdList.add(_parms.alias);
        cmdList.add("-keysize");
        cmdList.add(Integer.toString(_parms.keySize));
        cmdList.add("-storetype");
        cmdList.add("PKCS12");
        cmdList.add("-dname");
        cmdList.add("CN=" + _parms.user + ", OU=Texas Advanced Computing Center, O=University of Texas at Austin, L=Austin, ST=Texas, C=US");
        cmdList.add("-storepass");
        cmdList.add(_parms.password);
        
        // Create the process builder.
        System.out.println("Calling keytool to create \"" + _parms.alias + "\" key pair in " + storePath + ".");
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.redirectErrorStream(true);
        pb.inheritIO();
        Process process = pb.start();
        int rc = process.waitFor();
        
        // Make sure it worked.
        if (rc != 0) {
            String msg = MsgUtils.getMsg("TAPIS_EXTERNAL_PROCESS_FAILURE", "keytool", rc);
            _log.error(msg);
            throw new RuntimeException(msg);
        }
    }
    
    /* ---------------------------------------------------------------------- */
    /* retrieveKeys:                                                          */
    /* ---------------------------------------------------------------------- */
    private void retrieveKeys(KeyManager km) 
     throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, 
            InvalidKeyException, CertificateException, NoSuchProviderException, SignatureException
    {
        // ----- Get the private key from the keystore.
        System.out.println("Retrieving private key and certificate containing public key.");
        PrivateKey privateKey = km.getPrivateKey(_parms.alias, _parms.password);
        Certificate cert = km.getCertificate(_parms.alias);
        PublicKey publicKey = cert.getPublicKey();
        
        // ----- Verify certificate.
        System.out.println("Verifying the certificate.");
        cert.verify(publicKey);
        KeyPair keyPair = new KeyPair(publicKey, privateKey);
        
        // Reusable codec.
        Encoder b64Encoder = Base64.getUrlEncoder();
        
        // Print keys.
        System.out.println("**** Public Key Information (base64url encoded)");
        System.out.println("  algorithm: " + keyPair.getPublic().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPublic().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPublic().getEncoded()));
        System.out.println("**** End Public Key Information");
        System.out.println("**** Private Key Information (base64url encoded)");
        System.out.println("  algorithm: " + keyPair.getPrivate().getAlgorithm());
        System.out.println("  format   : " + keyPair.getPrivate().getFormat());
        System.out.println("  key      : " + b64Encoder.encodeToString(keyPair.getPrivate().getEncoded()));
        System.out.println("**** End Private Key Information\n");
    }
    
    /* ---------------------------------------------------------------------------- */
    /* confirmKeyReplace:                                                           */
    /* ---------------------------------------------------------------------------- */
    /** Confirm that the user wants to replace an existing key.
     * 
     * @return true to replace, false to abort.
     */
    private boolean confirmKeyReplace()
    {
      // Construct prompt.
      String prompt = "A key with the name " + _parms.alias + " already exists.\n" +
                      "Do you want to replace it? [y/N]: ";
      
      // Get the console.
      Console console = System.console();
      
      // Normal command line execution.
      if (console != null) 
      {
        // Use console facilities to hide password.
        console.printf("%s", prompt);
        String response = console.readLine();
        if (!StringUtils.isBlank(response)) {
            if (response.toLowerCase().equals("y")) return true;
              else return false;
        }
      }
      
      // When no console is available (like in Eclipse),
      // try using stdin and stdout.
      System.out.print(prompt);
      byte[] bytes = new byte[256];
      try {   
          // Read the input bytes which are not masked.
          int bytesread = System.in.read(bytes);
          String response = new String(bytes);
          if (!StringUtils.isBlank(response)) {
              if (response.toLowerCase().equals("y")) return true;
                else return false;
          }
        }
        catch (IOException e){}
      
      // We failed to get input.
      return false;
    }
}
