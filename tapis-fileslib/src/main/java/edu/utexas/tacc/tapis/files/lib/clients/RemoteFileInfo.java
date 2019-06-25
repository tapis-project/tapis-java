package edu.utexas.tacc.tapis.files.lib.clients;

import java.time.Instant;
import software.amazon.awssdk.services.s3.model.S3Object;

public class RemoteFileInfo {

    private Instant lastModified;
    private String name;
    private Long size;

    public RemoteFileInfo(S3Object listing) {
        this.name = listing.key();
        this.lastModified = listing.lastModified();
        this.size = listing.size();

    }
}
