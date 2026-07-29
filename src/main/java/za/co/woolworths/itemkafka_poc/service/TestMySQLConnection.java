package za.co.woolworths.itemkafka_poc.service;

import java.sql.Connection;
import java.sql.DriverManager;

public class TestMySQLConnection {
    public static boolean testConnection() {
        String url = "jdbc:mysql://localhost:3306/cs_caissa_central_master_data?useSSL=false&allowPublicKeyRetrieval=true";
        String user = "root";
        String password = "Anton@Perfect07";

        try (Connection connection = DriverManager.getConnection(url, user, password))
        {
            return true; // Connection was successful
        }
        catch (Exception e)
        {
            e.printStackTrace(); // Log the exception or handle accordingly
            return false; // Connection failed
        }
    }
}
