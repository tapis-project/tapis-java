package edu.utexas.tacc.tapis.files.lib.clients;

import edu.utexas.tacc.tapis.files.lib.models.StorageSystem;
import edu.utexas.tacc.tapis.shared.exceptions.TapisException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S3DataClient implements IRemoteDataClient {

    private S3Client client;

    public S3DataClient(StorageSystem system) throws IOException {
        AwsCredentials credentials = AwsBasicCredentials.create("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY");

        client = S3Client.builder()
                .build();
    }

    @Override
    public List<RemoteFileInfo> ls(String path) throws IOException {

        return new ArrayList<>();
    }
}
