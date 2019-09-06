package edu.utexas.tacc.tapis.files.lib.kernel;

import java.io.IOException;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class TestKernel {

public static void main(String[] args) throws SftpException, JSchException, IOException {
		
		String user = args[0];
		String host = args[1];
		int port = 22;
		//String privateKey = args[2];
		//String publicKey = args[3];
		String password = args[2];
		String LOCALFILE = args[3];
		String REMOTEFILE = args[4];
		
		//SftpFilesKernel sftp = new SftpFilesKernel(host, user, port, publicKey, privateKey);
		SftpFilesKernel sftp = new SftpFilesKernel(host, port, user, password);
		sftp.openSession();
		sftp.put(LOCALFILE, REMOTEFILE);
 
	}

}
