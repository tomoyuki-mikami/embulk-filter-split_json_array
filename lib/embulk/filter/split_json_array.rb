Embulk::JavaPlugin.register_filter(
  "split_json_array", "org.embulk.filter.split_json_array.SplitJsonArrayFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
