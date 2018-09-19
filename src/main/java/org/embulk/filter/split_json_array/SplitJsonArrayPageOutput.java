package org.embulk.filter.split_json_array;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.config.Task;
import org.embulk.spi.*;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import static org.embulk.filter.split_json_array.SplitJsonArrayFilterPlugin.PluginTask;

public class SplitJsonArrayPageOutput implements PageOutput {
    private class ArrayColumn {
        private final Column column;
        private final Optional<TimestampParser> timestampParser;

        ArrayColumn(Column column, Optional<TimestampParser> timestampParser) {
            this.column = column;
            this.timestampParser = timestampParser;
        }

        public Column getColumn() {
            return column;
        }

        public Optional<TimestampParser> getTimestampParser() {
            return timestampParser;
        }
    }

    private final Logger logger = Exec.getLogger(SplitJsonArrayPageOutput.class);
    private final PluginTask task;
    private final Schema inputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final List<ArrayColumn> inputColumnList;
    private final List<ArrayColumn> outputColumnList;
    private final Column jsonArrayColumn;
    private final JsonParser jsonParser = new JsonParser();

    SplitJsonArrayPageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput pageOutput) {
        this.task = task;
        this.inputSchema = inputSchema;
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.inputColumnList = initializeColumns(task, inputSchema);
        this.outputColumnList = initializeColumns(task, outputSchema);
        this.jsonArrayColumn = getJsonArrayColumn();
    }

    private List<ArrayColumn> initializeColumns(PluginTask task, Schema schema) {
        ImmutableList.Builder<ArrayColumn> expandedJsonColumnsBuilder = ImmutableList.builder();
        for (Column outputColumn : schema.getColumns()) {
            boolean added = false;
            for (ColumnConfig columnConfig : task.getArrayColumns()) {
                if (outputColumn.getName().equals(columnConfig.getName())) {
                    TimestampParser timestampParser = null;
                    if (Types.TIMESTAMP.equals(columnConfig.getType())) {
                        final TimestampColumnOption columnOption = columnConfig.getOption().loadConfig(TimestampColumnOption.class);
                        timestampParser = new TimestampParser(task, columnOption);
                    }

                    ArrayColumn expandedColumn = new ArrayColumn(outputColumn, Optional.fromNullable(timestampParser));
                    expandedJsonColumnsBuilder.add(expandedColumn);
                    added = true;
                }
            }
            if (!added) {
                ArrayColumn expandedColumn = new ArrayColumn(outputColumn, null);
                expandedJsonColumnsBuilder.add(expandedColumn);
            }
        }
        return expandedJsonColumnsBuilder.build();
    }

    private interface TimestampColumnOption extends Task, TimestampParser.TimestampColumnOption { }

    private Column getJsonArrayColumn() {
        Column jsonColumn = null;
        for (Column column: inputSchema.getColumns()) {
            if (column.getName().contentEquals(task.getJsonArrayColumn())) {
                jsonColumn = column;
            }
        }
        return jsonColumn;
    }

    @Override
    public void add(Page page) {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            buildRecord();
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close() {
        pageReader.close();
        pageBuilder.close();
    }

    private void buildRecord() {
        if (!pageReader.isNull(jsonArrayColumn)) {
            String jsonString;
            if (jsonArrayColumn.getType().equals(Types.JSON)) {
                jsonString = pageReader.getJson(jsonArrayColumn).toJson();
            } else {
                jsonString = pageReader.getString(jsonArrayColumn);
            }
//            logger.info(jsonString);

            ObjectMapper objectMapper = new ObjectMapper();
            TypeFactory typeFactory = objectMapper.getTypeFactory();

            try {
                if (task.getIsKeyValueArray()) {
                    List<Map<String, Object>> list = objectMapper.readValue(jsonString, typeFactory.constructCollectionType(List.class, Map.class));
                    for (Map<String, Object> record : list) {
                        for (ArrayColumn arrayColumn : outputColumnList) {
                            Object value = null;
                            if (validateArrayColumn(arrayColumn) && record.containsKey(arrayColumn.getColumn().getName())) {
                                value = record.get(arrayColumn.getColumn().getName());
                                setValueBuilder(arrayColumn, value);
                            } else {
                                setDefaultValueBuilder(arrayColumn);
                            }

                        }
                        pageBuilder.addRecord();
                    }
                } else {
                    List<Object> list = objectMapper.readValue(jsonString, typeFactory.constructCollectionType(List.class, Object.class));
                    for (Object record : list) {
                        for (ArrayColumn arrayColumn : outputColumnList) {
                            if (validateArrayColumn(arrayColumn)) {
                                setValueBuilder(arrayColumn, record);
                            } else {
                                setDefaultValueBuilder(arrayColumn);
                            }
                        }
                        pageBuilder.addRecord();
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean validateArrayColumn(ArrayColumn column) {
        // input に存在するカラムだったら false を返す
        for (ArrayColumn inputColumn : inputColumnList) {
            if (column.getColumn().getName().contentEquals(inputColumn.getColumn().getName())) {
                return false;
            }
        }
        return true;
    }

    private class JsonValueInvalidException extends DataException {
        JsonValueInvalidException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private void setDefaultValueBuilder(ArrayColumn arrayColumn) {
        if (Types.STRING.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setString(arrayColumn.getColumn(), pageReader.getString(arrayColumn.getColumn()));
        } else if (Types.BOOLEAN.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setBoolean(arrayColumn.getColumn(), pageReader.getBoolean(arrayColumn.getColumn()));
        } else if (Types.DOUBLE.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setDouble(arrayColumn.getColumn(), pageReader.getDouble(arrayColumn.getColumn()));
        } else if (Types.LONG.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setLong(arrayColumn.getColumn(), pageReader.getLong(arrayColumn.getColumn()));
        } else if (Types.TIMESTAMP.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setTimestamp(arrayColumn.getColumn(), pageReader.getTimestamp(arrayColumn.getColumn()));
        } else if (Types.JSON.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setJson(arrayColumn.getColumn(), pageReader.getJson(arrayColumn.getColumn()));
        }
    }

    private void setValueBuilder(ArrayColumn arrayColumn, Object value) {
        if (Types.STRING.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setString(arrayColumn.getColumn(), value.toString());
        } else if (Types.BOOLEAN.equals(arrayColumn.getColumn().getType())) {
            pageBuilder.setBoolean(arrayColumn.getColumn(), Boolean.parseBoolean(value.toString()));
        } else if (Types.DOUBLE.equals(arrayColumn.getColumn().getType())) {
            try {
                pageBuilder.setDouble(arrayColumn.getColumn(), Double.parseDouble(value.toString()));
            } catch (NumberFormatException e) {
                throw new JsonValueInvalidException(String.format("Failed to parse '%s' as double", value.toString()), e);
            }
        } else if (Types.LONG.equals(arrayColumn.getColumn().getType())) {
            try {
                pageBuilder.setLong(arrayColumn.getColumn(), Long.parseLong(value.toString()));
            } catch (NumberFormatException e) {
                // ad-hoc workaround for exponential notation
                try {
                    pageBuilder.setLong(arrayColumn.getColumn(), (long) Double.parseDouble(value.toString()));
                } catch (NumberFormatException e2) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as long", value.toString()), e);
                }
            }
        } else if (Types.TIMESTAMP.equals(arrayColumn.getColumn().getType())) {
            if (arrayColumn.getTimestampParser().isPresent()) {
                TimestampParser parser = arrayColumn.getTimestampParser().get();
                try {
                    pageBuilder.setTimestamp(arrayColumn.getColumn(), parser.parse(value.toString()));
                }
                catch (TimestampParseException e) {
                    throw new JsonValueInvalidException(String.format("Failed to parse '%s' as timestamp", value.toString()), e);
                }
            } else {
                throw new RuntimeException("TimestampParser is absent for column:" + arrayColumn.getColumn().getName());
            }
        } else if (Types.JSON.equals(arrayColumn.getColumn().getType())) {
            try {
                pageBuilder.setJson(arrayColumn.getColumn(), jsonParser.parse(value.toString()));
            } catch (JsonParseException e) {
                throw new JsonValueInvalidException(String.format("Failed to parse '%s' as JSON", value.toString()), e);
            }
        }
    }
}
