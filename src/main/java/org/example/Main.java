package org.example;

import org.example.service.XmlDBService;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        try {
            System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8));
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5433/test_db", "admin", "admin");
            // Удаление содержимого таблицы
            try (Statement stmt = connection.createStatement()) {
                // Удаляем всю схему со всеми таблицами (CASCADE обязателен)
                stmt.execute("DROP SCHEMA public CASCADE");
                // Создаем схему заново, чтобы база была готова к работе
                stmt.execute("CREATE SCHEMA public");
                // Восстанавливаем права (опционально, актуально для Docker)
                stmt.execute("GRANT ALL ON SCHEMA public TO public");
            }
            // Создание сервиса
            XmlDBService xmlDBService = new XmlDBService();

            System.out.println("Названия таблиц в xml: " + xmlDBService.getTableNames() + "\n");

            // Запрос для создания и заполнения таблицы 'currencies'
            System.out.println("Запрос для создания и заполнения таблицы 'currencies'");
            System.out.println(xmlDBService.getTableDDL("currencies"));
            System.out.println();

            // Создание таблицы currencies на основе xml
            xmlDBService.update("currencies");

            // Вывод содержимого таблицы currencies
            printTable(connection, "currencies");

            // Создание таблицы categories на основе xml
            xmlDBService.update("categories");

            // Вывод содержимого таблицы categories
            printTable(connection, "categories");

            // Создание остальных таблиц (offers) на основе xml
            xmlDBService.update();

            // Наименования столбцов таблицы offers
            System.out.println("Колонки таблицы offers (из бд):");
            System.out.println(xmlDBService.getColumnNames("offers"));
            System.out.println();

            // Проверка, является ли колонка id уникальным значением таблицы offers
            System.out.println("Колонка id в таблице offers уникальна? " + xmlDBService.isColumnId("offers", "id"));

            // Проверка, является ли колонка vendorCode уникальным значением таблицы offers
            System.out.println("Колонка vendorCode в таблице offers уникальна? " + xmlDBService.isColumnId("offers", "vendorCode"));
            System.out.println();

            // Удаление колонки rate из currencies ля имитации 'появления' новой колонки в xml
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("ALTER TABLE currencies DROP rate");
            }

            // Проверка удаления колонки rate
            System.out.println("Таблица currencies после удаления столбца rate:");
            printTable(connection, "currencies");
            System.out.println();

            // Наименования столбцов таблицы offers
            System.out.println("Колонки таблицы currencies после удаления колонки:");
            System.out.println(xmlDBService.getColumnNames("currencies"));
            System.out.println();

            // При попытке обновления таблицы из xml должна возникнуть ошибка
            System.out.println("При попытке обновления таблицы из xml должна возникнуть ошибка");
            try {
                xmlDBService.update("currencies");
            } catch (Exception e) {
                System.out.println("Вызов xmlDBService.update(currencies) привёл к ошибке:");
                System.out.println(e);
                System.out.println();
            }

            // Запрос для возврата удаленной колонки rate из currencies
            System.out.println("sql запрос для добавления 'новой' колонки:");
            System.out.println(xmlDBService.getDDLChange("currencies"));
            System.out.println();

            // Обновление таблицы с добавлением недостающих столбцов (внутри вызывается xmlDBService.getDDLChange)
            xmlDBService.updateWithChange("currencies");
            System.out.println("Таблица currencies после обновления данных:");
            printTable(connection, "currencies");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printTable(Connection connection, String tableName) throws SQLException {
        System.out.println("Содержимое таблицы " + tableName + ":");
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData meta = resultSet.getMetaData();
        int cols = meta.getColumnCount();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= cols; i++) {
                sb.append(meta.getColumnName(i)).append(": ").append(resultSet.getObject(i)).append("\n");
            }
            System.out.println(sb);
        }
    }
}