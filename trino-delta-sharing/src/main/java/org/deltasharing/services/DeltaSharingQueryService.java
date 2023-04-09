package org.deltasharing.services;

import org.deltasharing.models.DeltaSharingQueryRequest;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

public interface DeltaSharingQueryService {

    @Streaming
    @Headers({"accept: application/json; charset=utf-8","Delta-Table-Version: {delta-version}"})
    @POST("shares/{share}/schemas/{schema}/tables/{table}/query")
    Call<ResponseBody> getTableData(
            @Header("delta-version") String deltaVersion,
            @Path("share") String share,
            @Path("schema") String schema,
            @Path("table") String table,
            @Body DeltaSharingQueryRequest deltaSharingQueryRequest);
}
