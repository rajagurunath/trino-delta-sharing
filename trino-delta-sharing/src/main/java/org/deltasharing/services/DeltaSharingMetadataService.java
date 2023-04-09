package org.deltasharing.services;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

public interface DeltaSharingMetadataService {

    @Streaming
    @Headers({"accept: application/json; charset=utf-8","Delta-Table-Version: {delta-version}"})
    @GET("shares/{share}/schemas/{schema}/tables/{table}/metadata")
    Call<ResponseBody> getTableMetadata(
            @Header("delta-version") String deltaVersion,
            @Path("share") String share,
            @Path("schema") String schema,
            @Path("table") String table
    );
}
