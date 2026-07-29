package za.co.woolworths.itemkafka_poc.util;

import za.co.woolworths.itemkafka_poc.model.Item;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ItemJdbcMapper{} /*implements JdbcMapper<Item> {
    @Override
    public PreparedStatement map(Item item, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, item.getItemId());
        preparedStatement.setInt(2, item.getItemLevel());
        preparedStatement.setString(3, item.getItemLongDesc()); // Add other fields as needed
        return preparedStatement;
    }
}*/
