package io.trino.deltasharing.services;

import io.trino.deltasharing.models.DeltaSharingTableResponse;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Path;

public interface DeltaSharingTableService {

    @Headers("accept: application/json; charset=utf-8")
    @GET("shares/{share}/schemas/{schema}/tables")
    Call<DeltaSharingTableResponse> getTables(
            @Path("share") String share,
            @Path("schema") String schema
    );

    @Headers("accept: application/json; charset=utf-8")
    @GET("shares/{share}/schemas/all-tables")
    Call<DeltaSharingTableResponse> getAllTables(
            @Path("share") String share
    );


}
