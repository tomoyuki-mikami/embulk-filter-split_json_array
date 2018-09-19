package org.embulk.filter.split_json_array;

import com.google.common.collect.ImmutableList;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.time.TimestampParser;
import org.slf4j.Logger;

import java.util.List;

public class SplitJsonArrayFilterPlugin implements FilterPlugin {
    private static final Logger logger = Exec.getLogger(SplitJsonArrayFilterPlugin.class);

    public interface PluginTask extends Task, TimestampParser.Task {
        @Config("json_array_column")
        public String getJsonArrayColumn();

        @Config("is_key_value_array")
        public boolean getIsKeyValueArray();

        @Config("array_columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getArrayColumns();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        validateDuplicateColumn(task, inputSchema);
        Schema outputSchema = buildOutputSchema(task, inputSchema);

        control.run(task.dump(), outputSchema);
    }

    private void validateDuplicateColumn(PluginTask task, Schema inputSchema) {
        for (Column inputColumn : inputSchema.getColumns()) {
            for (ColumnConfig arrayColumnConfig : task.getArrayColumns()) {
                if (inputColumn.getName().contentEquals(arrayColumnConfig.getName())) {
                    throw new ConfigException(String.format("duplicate Column -> %s", inputColumn.getName()));
                }
            }
        }
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new SplitJsonArrayPageOutput(task, inputSchema, outputSchema, output);
    }

    private Schema buildOutputSchema(PluginTask task, Schema inputSchema) {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0; // columns index

        // ベースカラムが先
        for (Column inputColumn: inputSchema.getColumns()) {
            if (!inputColumn.getName().contentEquals(task.getJsonArrayColumn())) {
                logger.info("base column: name: {}, type: {}, index: {}",
                        inputColumn.getName(),
                        inputColumn.getType(),
                        i);
                Column outputColumn = new Column(i++,
                        inputColumn.getName(),
                        inputColumn.getType());
                builder.add(outputColumn);
            }
        }

        // json array で指定したカラム

        for (Column inputColumn: inputSchema.getColumns()) {
            if (inputColumn.getName().contentEquals(task.getJsonArrayColumn())) {
                if (task.getIsKeyValueArray()) {
                    for (ColumnConfig columnConfig: task.getArrayColumns()) {
                        logger.info("key-value array column: name: {}, type: {}, options: {}, index: {}",
                                columnConfig.getName(),
                                columnConfig.getType(),
                                columnConfig.getOption(),
                                i);
                        Column outputColumn = new Column(i++,
                                columnConfig.getName(),
                                columnConfig.getType());
                        builder.add(outputColumn);
                    }
                } else {
                    if (task.getArrayColumns().size() == 1) {
                        ColumnConfig columnConfig = task.getArrayColumns().get(0);
                        logger.info("simple array column: name: {}, type: {}, options: {}, index: {}",
                                columnConfig.getName(),
                                columnConfig.getType(),
                                columnConfig.getOption(),
                                i);
                        Column outputColumn = new Column(i++,
                                columnConfig.getName(),
                                columnConfig.getType());
                        builder.add(outputColumn);
                    }
                }
            }
        }

        return new Schema(builder.build());
    }
}
