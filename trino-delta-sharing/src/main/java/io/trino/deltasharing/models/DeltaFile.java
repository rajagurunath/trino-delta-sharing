package io.trino.deltasharing.models;

public class DeltaFile {
    public String url;
    public String id;
    public PartitionValues partitionValues;
    public int size;
    public DeltaSharingStats stats;
}

