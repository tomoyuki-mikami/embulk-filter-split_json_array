# Split Json Array filter plugin for Embulk

## Overview

* **Plugin type**: filter

## Configuration

- **json_array_column**: a column name having json array (string, required)
- **array_columns**: json array columns (array of hash, required)
    * name: name of the column
    * type: type of the column
    * format: format of the timestamp if type is timestamp
    * timezone: Time zone of each timestamp columns if values don’t include time zone description (UTC by default)

## Example

```yaml
filters:
  - type: split_json_array
    json_array_column: data
    array_columns:
      - { name: number, type: long }
      - { name: message, type: string }
      - { name: created_at, type: timestamp, format: "%Y-%m-%d %H:%M:%S %z", timezone: "JST" }
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
