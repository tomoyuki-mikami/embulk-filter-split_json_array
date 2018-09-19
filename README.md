# Split Json Array filter plugin for Embulk

## Overview

* **Plugin type**: filter

## Configuration

- **json_array_column**: a column name having json array (string, required)
- **is_key_value_array**: 配列内が key-value かどうか (boolean, required)
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
    is_key_value_array: true
    array_columns:
      - { name: number, type: long }
      - { name: message, type: string }
      - { name: created_at, type: timestamp, format: "%Y-%m-%d %H:%M:%S %z", timezone: "JST" }
```

## できること
ネスト化された json 内の配列を縦方向に展開できます  
このプラグインを利用する前に embulk-filter-expand_json を挟んでください  

2種類の配列に対応しています  
- example/data1.json のような key-value の配列
    - is_key_value_array: true 
- example/data2.json のような単純な配列
    - is_key_value_array: false  
    
is_key_value_array: false の場合に指定する array_columns は  
name が出力カラム名、 type は配列内の値の型です  
2つ以上指定してしまった場合は動きません    


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
