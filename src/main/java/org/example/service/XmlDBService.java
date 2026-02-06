package org.example.service;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.GPathResult;
import groovy.xml.slurpersupport.NodeChild;
import org.example.schema.TableSchema;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import org.json.JSONObject;

public class XmlDBService {
    private final GPathResult yml_catalog;
    private final HashMap<String, TableSchema> tableSchemas = new HashMap<>();
    private final Connection sqlConnection;

    public XmlDBService() throws ParserConfigurationException, SAXException, IOException, SQLException {
        XmlSlurper xmlSlurper = new XmlSlurper(false, true);
        xmlSlurper.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
        xmlSlurper.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "all");
        xmlSlurper.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "all");
        xmlSlurper.setEntityResolver((publicId, systemId) ->
                new org.xml.sax.InputSource(new java.io.StringReader(""))
        );
        String httpsUrl = "https://expro.ru/bitrix/catalog_export/export_Sai.xml";
        URL url = URI.create(httpsUrl).toURL();
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept", "application/xml");
        InputStream xmlStream = connection.getInputStream();
        yml_catalog = xmlSlurper.parse(xmlStream);
        sqlConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/test_db", "admin", "admin");
        sqlConnection.setAutoCommit(false);
        loadSchemasFromDb();
    }

    /**
     * Возвращает названия таблиц из XML (currency, categories, offers)
     * @return названия всех таблиц
     */
    public ArrayList<String> getTableNames() {
        ArrayList<String> result = new ArrayList<>();
        GPathResult shop = (GPathResult)yml_catalog.getProperty("shop");
        // Итерируемся по всем элементам shop
        for (Object node: shop.children()) {
            GPathResult child = (GPathResult) node;
            // Если внутри элемента присутствуют другие элементы, считаем, что это таблица
            if(!child.children().isEmpty()) result.add(child.name());
        }
        return result;
    }

    /**
     * Создает sql для создания таблиц динамически из XML
     * @param tableName название таблицы
     * @return sql для создания таблиц
     */
    public String getTableDDL(String tableName) {
        StringBuilder sqlQueryBuilder = new StringBuilder();

        TableSchema tableSchema = tableSchemas.get(tableName);
        if (tableSchema == null) {
            tableSchema = getTableSchema(tableName);
            sqlQueryBuilder.append(tableSchema.toSql()).append("\n");
        }

        GPathResult shop = (GPathResult)yml_catalog.getProperty("shop");
        GPathResult tableContent = (GPathResult)shop.getProperty(tableName);
        List<String> columnNames = tableSchema.getColumnNames();

        List<String> valuesRows = new ArrayList<>();

        for (Object row: tableContent.children()) {
            NodeChild rowNodeChild = (NodeChild)row;
            @SuppressWarnings("unchecked")
            Map<String, String> originalAttrs = (Map<String, String>) rowNodeChild.attributes();

            Map<String, String> valuesMap = originalAttrs.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().toLowerCase(),
                            Map.Entry::getValue,
                            (oldValue, newValue) -> newValue
                    ));
            Map<String, String> paramMap = new HashMap<>();


            for (Object child: rowNodeChild.children()) {
                NodeChild column = (NodeChild)child;
                String columnName = column.name().toLowerCase();

                // Для полей "param" создаётся отдельный HashMap, который затем приводится к формату jsonb
                if (Objects.equals(columnName, "param")) {
                    String paramName = (String) column.attributes().get("name");
                    paramMap.put(paramName, column.text());
                } else {
                    valuesMap.put(columnName, column.text());
                }
            }

            if (!paramMap.isEmpty()) valuesMap.put("param", new JSONObject(paramMap).toString());

            // Для данных в строчках таблицы без имени (как в categories) добавляется название колонки "data"
            if (rowNodeChild.children().isEmpty() & !Objects.equals(rowNodeChild.text(), "")) {
                valuesMap.put("data", rowNodeChild.text());
            }

            // Генерация запроса для вставки одной строки
            StringJoiner rowJoiner = new StringJoiner(", ", "(", ")");
            for (String columnName : columnNames) {
                String value = valuesMap.get(columnName);
                if (value == null) rowJoiner.add("NULL");
                else {
                    if (columnName.equals("param")) {
                        rowJoiner.add("'" + value + "'::jsonb");
                    } else {
                        rowJoiner.add("'" + value + "'");
                    }
                }

            }
            valuesRows.add(rowJoiner.toString());
        }


        if (!valuesRows.isEmpty()) {
            // Часть запроса с созданием виртуальной таблицы для поиска новых/поменявшихся записей
            sqlQueryBuilder.append("WITH xml_data (").append(String.join(", ", columnNames)).append(") AS (\n");
            sqlQueryBuilder.append("  VALUES ").append(String.join(",\n         ", valuesRows)).append("\n),\n");

            // Удаление записей, которых нет в новом XML
            sqlQueryBuilder.append("deleted AS (\n");
            sqlQueryBuilder.append("  DELETE FROM ").append(tableName).append(" WHERE id NOT IN (SELECT id FROM xml_data)\n) \n");

            // Вставка в таблицу записей или их обновление
            sqlQueryBuilder.append("INSERT INTO ").append(tableName).append(" (").append(String.join(", ", columnNames)).append(")\n");
            sqlQueryBuilder.append("SELECT * FROM xml_data\n");
            sqlQueryBuilder.append("ON CONFLICT (id) DO UPDATE SET ");

            // Формирование блока обновления для ON CONFLICT
            StringJoiner updateSet = new StringJoiner(", ");
            for (String col : columnNames) {
                if (!col.equalsIgnoreCase("id")) {
                    updateSet.add(col + " = EXCLUDED." + col);
                }
            }

            sqlQueryBuilder.append(updateSet).append(";");
        } else {
            // Удаление всех записей таблицы, если xml пустой
            sqlQueryBuilder.append("DELETE FROM ").append(tableName).append(";");
        }
        return sqlQueryBuilder.toString();
    }

    /**
     * Обновляет данные в таблицах бд
     * если поменялась структура выдает exception
     */
    public void update() throws Exception {
        ArrayList<String> tableNames = getTableNames();
        for (String tableName: tableNames) update(tableName);
    }

    /**
     * Обновляет данные в таблице бд
     * если поменялась структура выдает exception
     * @param tableName название таблицы
     */
    public void update(String tableName) throws Exception {
        loadSchemasFromDb();
        TableSchema oldTableSchema = tableSchemas.get(tableName);
        TableSchema newTableSchema = getTableSchema(tableName);
        if (oldTableSchema != null & !newTableSchema.equals(oldTableSchema)) {
            throw new Exception("Schema change in table " + tableName + "!");
        }
        String tableDDL = getTableDDL(tableName);

        try (Statement stmt = sqlConnection.createStatement()) {
            stmt.execute(tableDDL);
            sqlConnection.commit();
            TableSchema tableSchema = getTableSchema(tableName);
            tableSchemas.put(tableName, tableSchema);
        } catch (SQLException e) {
            sqlConnection.rollback();
            throw e;
        }
    }

    /**
     * Обновляет данные в таблицах бд
     * если появились новые столбцы, добавляет их
     * если есть недостающие столбцы, выдает exception
     */
    public void updateWithChange() throws Exception {
        ArrayList<String> tableNames = getTableNames();
        for (String tableName: tableNames) updateWithChange(tableName);
    }

    /**
     * Обновляет данные в таблице бд
     * если появились новые столбцы, добавляет их
     * если есть недостающие столбцы, выдает exception
     */
    public void updateWithChange(String tableName) throws Exception {
        loadSchemasFromDb();
        String tableChangeDDL = getDDLChange(tableName);
        if (tableChangeDDL != null) {
            TableSchema newTableSchema = getTableSchema(tableName);
            try (Statement stmt = sqlConnection.createStatement()) {
                stmt.executeUpdate(tableChangeDDL);
                sqlConnection.commit();
            } catch (SQLException e) {
                sqlConnection.rollback();
                throw e;
            }
            tableSchemas.put(tableName, newTableSchema);
        }

        update(tableName);
    }

    /** Возвращает наименования всех столбцов таблицы
     * @param tableName наименование таблицы
     * @return наименования всех столбцов таблицы
     */
    public ArrayList<String> getColumnNames(String tableName) throws SQLException {
        loadSchemasFromDb();
        TableSchema tableSchema = tableSchemas.get(tableName);
        return tableSchema.getColumnNames();
    }

    /**
     * Возвращает true, если столбец не имеет повторяющихся значений
     * @param tableName название таблицы
     * @param columnName название столбца
     * @return true, если столбец не имеет повторяющихся значений, иначе false
     */
    public boolean isColumnId(String tableName, String columnName) throws SQLException {
        String query = String.format("SELECT COUNT(DISTINCT %s) = COUNT(*) AS is_unique FROM %s", columnName, tableName);
        Statement statement = sqlConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
        return resultSet.getBoolean("is_unique");
    }

    /**
     * Возвращает sql запрос для создания новых столбцов таблицы на основе xml
     * @param tableName название таблицы
     * @return sql запрос для создания новых столбцов таблицы.
     * Примечание: создаются пустые колонки. Для их заполнения использовать после создания столбцов update(tableName).
     */
    public String getDDLChange(String tableName) throws Exception {
        TableSchema oldTableSchema = tableSchemas.getOrDefault(tableName, new TableSchema(tableName));
        TableSchema newTableSchema = getTableSchema(tableName);
        return oldTableSchema.newColumnsSql(newTableSchema);
    }

    /**
     * Возвращает схему таблицы из xml
     * @param tableName название таблицы
     * @return схема таблицы
     */
    private TableSchema getTableSchema(String tableName) {
        GPathResult shop = (GPathResult)yml_catalog.getProperty("shop");
        GPathResult tableContent = (GPathResult)shop.getProperty(tableName);

        // Список ключей в тегах записей
        Set<String> tagKeySet = new HashSet<>();

        for (Object row: tableContent.children()) {
            // Обновление списка ключей тегов (и nullable тегов) записей
            NodeChild rowNodeChild = (NodeChild)row;
            Set<String> newTagNames = new HashSet<>();

            // Добавляем в записи теги в качестве ключей
            for (Object child: rowNodeChild.children()) {
                newTagNames.add(((GPathResult) child).name());
            }

            // Если в записи нет тегов, но есть содержимое, добавляем ключ "data"
            if (newTagNames.isEmpty() & !Objects.equals(rowNodeChild.text(), "")) newTagNames.add("data");
            @SuppressWarnings("unchecked")
            Map<String, String> attributes = (Map<String, String>)(rowNodeChild).attributes();
            Set<String> newAttributeKeys = new HashSet<>(attributes.keySet());
            newAttributeKeys.addAll(newTagNames);
            tagKeySet.addAll(newAttributeKeys);
        }

        // Сортировка колонок для сохранения их порядка
        List<String> tagKeyList = tagKeySet.stream().toList();
        return getTableSchema(tableName, tagKeyList);
    }

    private static TableSchema getTableSchema(String tableName, List<String> tagKeyList) {
        List<String> tagKeyConstrains = new ArrayList<>();
        List<String> tagKeyTypes = new ArrayList<>();
        for (String key: tagKeyList) {
            String keyLowerCase = key.toLowerCase();
            if (Objects.equals(keyLowerCase, "param")) tagKeyTypes.add("jsonb");
            else tagKeyTypes.add("varchar");
            if (Objects.equals(keyLowerCase, "id")) tagKeyConstrains.add("PRIMARY KEY");
            else tagKeyConstrains.add("");
        }

        int size = Math.min(tagKeyList.size(), Math.min(tagKeyConstrains.size(), tagKeyTypes.size()));
        TableSchema tableSchema = new TableSchema(tableName);
        for (int i = 0; i < size; i++) {
            tableSchema.addColumn(tagKeyList.get(i).toLowerCase(), tagKeyTypes.get(i), tagKeyConstrains.get(i));
        }
        tableSchema.sortColumns();
        return tableSchema;
    }

    private void loadSchemasFromDb() throws SQLException {
        DatabaseMetaData meta = sqlConnection.getMetaData();

        // 1. Получаем список всех пользовательских таблиц
        try (ResultSet rsTables = meta.getTables(null, "public", null, new String[]{"TABLE"})) {
            while (rsTables.next()) {
                String tableName = rsTables.getString("TABLE_NAME");
                TableSchema schema = new TableSchema(tableName);

                Set<String> pkColumns = new HashSet<>();
                try (ResultSet pks = meta.getPrimaryKeys(null, "public", tableName)) {
                    while (pks.next()) {
                        pkColumns.add(pks.getString("COLUMN_NAME"));
                    }
                }
                // 2. Для каждой таблицы получаем список её колонок
                try (ResultSet rsColumns = meta.getColumns(null, "public", tableName, null)) {

                    while (rsColumns.next()) {
                        String columnName = rsColumns.getString("COLUMN_NAME");
                        String columnType = rsColumns.getString("TYPE_NAME");

                        // Формируем ограничения (constraints)
                        String constraints = pkColumns.contains(columnName) ? "PRIMARY KEY" : "";

                        // Добавляем колонку в схему
                        schema.addColumn(columnName.toLowerCase(), columnType, constraints);
                    }
                }

                schema.sortColumns();

                // 3. Сохраняем в вашу карту
                tableSchemas.put(tableName, schema);
            }
        }
    }
}
