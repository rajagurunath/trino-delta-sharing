package io.trino.deltasharing;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.StandardTypes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

//@ScalarFunction(value = DeltaSharingCleaner.NAME)
public final class DeltaSharingCleaner
{
    public static final String NAME = "delta_sharing_clean_up";

    @ScalarFunction(value=NAME, deterministic = true)
    @Description("Clean's up the delta Sharing parquet directory")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean CleanUp(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice directory,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice schemaName,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice tableName) throws IOException {
        System.out.println("================= cleanup =================================================================");
        System.out.println(directory);
        System.out.println(schemaName.toStringUtf8() + tableName.toStringUtf8());
        cleanUp(directory.toStringUtf8() +schemaName.toStringUtf8()+"."+tableName.toStringUtf8());
        return true;
    }

    public static void cleanUp(String directory) throws IOException {
        System.out.println(directory);
        Path dir = Paths.get(directory); //path to the directory
        Files
                .walk(dir) // Traverse the file tree in depth-first order
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        System.out.println("Deleting: " + path);
                        Files.delete(path);  //delete each file or directory
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }


}
