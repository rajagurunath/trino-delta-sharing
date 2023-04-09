package org.deltasharing.models;

import java.util.List;

public class DeltaSharingSchemaResponse {
//    private Map<String,List<DeltaSharingSchema>> schemas;
    private List<DeltaSharingSchema> items;
    private String nextPageToken;

    public List<DeltaSharingSchema> getItems() {
        return items;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }

    public List<String> getSchemas() {
        return items.stream().map(s->s.getSchema()).toList();
    }
//
//    //Setters and getters
//
//    public String toString() {
//        return "DeltaSharingSchemaResponse [schema=" + schemas + "]";
//    }
}
