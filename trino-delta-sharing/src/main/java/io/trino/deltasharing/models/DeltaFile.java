package io.trino.deltasharing.models;

public class DeltaFile {
    public String url;
    public String id;
    public PartitionValues partitionValues;
    public int size;
    // differs between Lakehouse Sharing and delta-sharing new versions (String vs DeltaSharingStats)
    public String stats;
}

