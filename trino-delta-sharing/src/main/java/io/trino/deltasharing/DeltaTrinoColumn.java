package io.trino.deltasharing;
import io.trino.spi.type.*;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;

public class DeltaTrinoColumn {
    public static Type toParquetType(TypeOperators typeOperators, Type type)
    {
        if (type instanceof TimestampWithTimeZoneType timestamp) {
            verify(timestamp.getPrecision() == 3, "Unsupported type: %s", type);
            return TIMESTAMP_MILLIS;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(toParquetType(typeOperators, arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(toParquetType(typeOperators, mapType.getKeyType()), toParquetType(typeOperators, mapType.getValueType()), typeOperators);
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> RowType.field(field.getName().orElseThrow(), toParquetType(typeOperators, field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }

    public static String cleanSparkColumn(String columnName){
        final String regex = "\\([^)]*\\)";

        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(columnName);

        return matcher.replaceAll("");
    }
    public static Type convert(String sparkType) {
        switch (cleanSparkColumn(sparkType)) {
            case "boolean":
                return BooleanType.BOOLEAN;
            case "tinyint":
                return TinyintType.TINYINT;
            case "smallint":
                return SmallintType.SMALLINT;
            case "integer":
            case "int":
                return IntegerType.INTEGER;
            case "bigint":
            case "long":
                return BigintType.BIGINT;
            case "float":
                return RealType.REAL;
            case "double":
                return DoubleType.DOUBLE;
            case "decimal":
                return DecimalType.createDecimalType();
            case "string":
            case "char":
            case "varchar":
            case "text":
                return VarcharType.VARCHAR;
            case "timestamp":
                return TimestampType.TIMESTAMP_NANOS;
            case "date":
                return DateType.DATE;
//            case "array":
//                return new ArrayType(convert(sparkType));
//            case "map":
//                return MapType.fromTypes(convert(sparkType.keyType()), convert(sparkType.valueType()));
//            case "struct":
//                List<Type> trinoFieldTypes = new ArrayList<>();
//                List<String> fieldNames = new ArrayList<>();
//                for (StructField field : sparkType.fields()) {
//                    trinoFieldTypes.add(convert(field.dataType()));
//                    fieldNames.add(field.name());
//                }
//                return RowType.from(trinoFieldTypes, fieldNames);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + sparkType);
        }
    }
}