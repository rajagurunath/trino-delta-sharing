package io.trino.deltasharing.models;

import java.util.List;


public class DeltaSharingTableResponse {
    //    private Map<String,List<DeltaSharingSchema>> schemas;
    private List<DeltaSharingTableModel> items;
    private String nextPageToken;

    public List<DeltaSharingTableModel> getItems() {
        return items;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }

    public List<String> getSchemas() {
        return items.stream().map(DeltaSharingTableModel::getSchema).toList();
    }

    public List<String> getTables() {
        return items.stream().map(DeltaSharingTableModel::getTable).toList();
    }

//
//    //Setters and getters
//
//    public String toString() {
//        return "DeltaSharingSchemaResponse [schema=" + schemas + "]";
//    }
}
