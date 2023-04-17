package io.trino.deltasharing.models;

public class DeltaSharingTableModel {
    private String name;
    private String share;
    private String schema;
    private String shareId;
    private String id;

    public String getSchema(){
        return schema;
    }

    public String getShare(){
        return share;
    }

    public String getTable(){
        return name;
    }
}
