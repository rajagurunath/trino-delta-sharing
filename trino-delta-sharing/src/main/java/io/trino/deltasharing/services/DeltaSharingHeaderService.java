package io.trino.deltasharing.services;

import retrofit2.Call;
import retrofit2.http.HEAD;
import retrofit2.http.Headers;
import retrofit2.http.Path;

public interface DeltaSharingHeaderService {

    @Headers("accept: application/json; charset=utf-8")
    @HEAD("shares/{share}/schemas/{schema}/tables/{table}")
    Call<Void> getTableVersion(
            @Path("share") String share,
            @Path("schema") String schema,
            @Path("table") String table
    );

}
