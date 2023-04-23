package io.trino.deltasharing.models;

import java.util.List;

public class DeltaSharingQueryRequest {
    public List<String> predicateHints;
    public String limitHint;
    public String version;

    public DeltaSharingQueryRequest(
            List<String> predicateHints,
           String limitHint,
           String version
            )
    {
        this.predicateHints = predicateHints;
        this.limitHint = limitHint;
        this.version = version;

    }

}
