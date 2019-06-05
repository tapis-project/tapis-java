package edu.utexas.tacc.tapis.files.lib.clients;

import java.util.Date;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

public class RemoteFileInfo {

    private Date lastModified;
    private String name;
    private Long size;

    public RemoteFileInfo(ListBucketsResponse listing) {

    }
}
