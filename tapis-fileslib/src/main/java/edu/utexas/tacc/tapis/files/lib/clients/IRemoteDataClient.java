package edu.utexas.tacc.tapis.files.lib.clients;

import java.io.IOException;
import java.util.List;

public interface IRemoteDataClient {

    List<RemoteFileInfo> ls(String remotePath) throws IOException;
    Boolean move(String srcSystem, String srcPath, String destSystem, String destPath);
}