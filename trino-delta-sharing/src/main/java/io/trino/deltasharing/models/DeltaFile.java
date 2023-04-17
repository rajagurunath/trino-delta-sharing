package io.trino.deltasharing.models;

import java.util.Map;

public class DeltaFile {
    public String url;
    public String id;
    public Map<String,String> partitionValues;
    public int size;
    public DeltaSharingStats stats;
}

