package io.trino.deltasharing;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;

public enum DeltaSharingErrorCode implements ErrorCodeSupplier {

    DELTA_SHARING_BAD_DATA(2, EXTERNAL),
    // HUDI_MISSING_DATA(3, EXTERNAL) is deprecated
    DELTA_SHARING_CANNOT_OPEN_SPLIT(4, EXTERNAL),
    DELTA_SHARING_UNSUPPORTED_FILE_FORMAT(5, EXTERNAL),
    DELTA_SHARING_CURSOR_ERROR(6, EXTERNAL);
    private final ErrorCode errorCode;

    DeltaSharingErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0507_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
