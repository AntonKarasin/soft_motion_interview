package org.example.schema;

import java.util.*;
import java.util.stream.Collectors;

public class TableSchema {
    String tableName;
    List<Column> columns = new ArrayList<>();

    public TableSchema(String tableName) {
        this.tableName = tableName;
    }

    record Column(String name, String type, String constraints) {}

    public void addColumn(String name, String type, String constraints) {
        this.columns.add(new Column(name, type, constraints));
    }

    public String toSql() {
        String cols = columns.stream()
                .map(c -> c.name + " " + c.type + " " + (c.constraints != null ? c.constraints : ""))
                .collect(Collectors.joining(",\n    "));

        return String.format("CREATE TABLE %s (\n    %s\n);", tableName, cols);
    }

    public String newColumnsSql(TableSchema newSchema) throws Exception {
        if (this.equals(newSchema)) return null;

        Set<String> currentNames = this.columns.stream()
                .map(Column::name)
                .collect(Collectors.toSet());

        List<Column> newColumns = newSchema.columns.stream()
                .filter(c -> !currentNames.contains(c.name()))
                .toList();

        if (newColumns.isEmpty()) {
            throw new Exception("В таблице из xml должны быть все колонки из бд");
        }

        String cols = newColumns.stream()
                .map(c -> "ADD COLUMN " + c.name + " " + c.type + " " + (c.constraints != null ? c.constraints : ""))
                .collect(Collectors.joining(",\n    "));

        return String.format("ALTER TABLE %s\n    %s;", tableName, cols);
    }

    public ArrayList<String> getColumnNames() {
        return columns.stream().map(Column::name).collect(Collectors.toCollection(ArrayList::new));
    }

    public void sortColumns() {
        this.columns.sort(Comparator.comparing(Column::name));
    }

    @Override
    public boolean equals(Object o) {
        // 1. Проверка на идентичность ссылок
        if (this == o) return true;

        // 2. Проверка на null и совпадение классов
        if (o == null || getClass() != o.getClass()) return false;

        // 3. Приведение типа
        TableSchema that = (TableSchema) o;

        // 4. Сравнение полей (имя таблицы и список колонок)
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        // Хеш-код должен переопределяться вместе с equals
        return Objects.hash(tableName, columns);
    }

    @Override
    public String toString() {
        return "TableSchema{" +
                "table='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
