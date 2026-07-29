package za.co.woolworths.itemkafka_poc.flink.jobs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.woolworths.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import za.co.woolworths.itemkafka_poc.model.Item;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j // Using Lombok's Slf4j annotation for logging
public class FlinkJob1_cmdLine
{

    private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            //.serializeNulls()          // This will serialize fields even with null values
            .create();

    public static void main(String[] args)
    {
        log.debug("Starting Flink Job 1 to connect to SQL Server and publish to Kafka."); // Log the start of the job

        // Using an existing execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String jdbcUrl = "jdbc:sqlserver://C21SQL04\\AMOS.1:1433;databaseName=CS_Caissa_Central_Master_Data;integratedSecurity=true;encrypt=false;";
        String query = "SELECT TOP 100 * FROM ITEM";

        List<Item> items = new ArrayList<>();

        // Fetch data from SQL Server
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query))
        {

            while (resultSet.next())
            {
                Item item = createItemFromResultSet(resultSet);
                items.add(item);
            }
            log.debug("Fetched {} items from SQL Server.", items.size());

        }
        catch (Exception e)
        {
            log.error("Error fetching data from SQL Server: {}", e.getMessage());
            return; // Exit if fetching data fails
        }

        // Set up Kafka Sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("Item_Topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        try
        {
            // Create a DataStream from the list of items
            DataStream<String> messageStream = env.fromData(items)
                    .map(new ItemToJsonFunction()) // Convert Item objects to JSON strings
                    .map(itemJson ->
                    {
                        log.debug("Publishing Item JSON to Kafka: {}", itemJson);
                        return itemJson;
                    });

            // Send the JSON strings to Kafka
            messageStream.sinkTo(kafkaSink);

            // Execute the Flink job
            env.execute("Flink Job 1: Read from SQL Server and Publish to Kafka");
            log.debug("Flink Job 1 executed successfully."); // Log job execution success

        }
        catch (Exception e)
        {
            log.error("Execution failed for Flink Job 1: {}", e.getMessage());
        }
    }

    private static Item createItemFromResultSet(ResultSet rs) throws Exception
    {
        Item item = new Item();
        item.setItemId(rs.getString("item_id"));
        item.setItemLevel(rs.getInt("item_level"));
        item.setItemNumberType(rs.getString("item_number_type"));
        item.setPrefix(rs.getInt("prefix"));
        item.setAllocatorSystem(rs.getString("allocator_system"));
        item.setBusinessUnitId(rs.getInt("business_unit_id"));
        item.setCatchWeightInd(rs.getString("catch_weight_ind"));
        item.setClassId(rs.getInt("class_id"));
        item.setColourDsc(rs.getString("colour_dsc"));
        item.setColourGroupId(rs.getString("colour_group_id"));
        item.setColourId(rs.getString("colour_id"));
        item.setColourRangeId(rs.getInt("colour_range_id"));
        item.setCompanyId(rs.getInt("company_id"));
        item.setCountOnUsId(rs.getString("count_on_us_id"));

        // Handle creation and last update date
        Timestamp createTimestamp = rs.getTimestamp("create_dte");
        if (createTimestamp != null)
        {
            item.setCreateDte(createTimestamp.toLocalDateTime()); // Assuming CreateDte is LocalDateTime in Item class
        }

        // Handle last update date
        Timestamp lastUpdateTimestamp = rs.getTimestamp("last_update_dte");
        if (lastUpdateTimestamp != null)
        {
            item.setLastUpdateDte(lastUpdateTimestamp.toLocalDateTime()); // Assuming LastUpdateDte is LocalDateTime in Item class
        }
        // Continue to set other fields...
        item.setDeptId(rs.getInt("dept_id"));
        item.setDiscipline(rs.getString("discipline"));
        item.setDomainId(rs.getInt("domain_id"));
        item.setFlavourDsc(rs.getString("flavour_dsc"));
        item.setFlavourGroupId(rs.getString("flavour_group_id"));
        item.setFlavourId(rs.getString("flavour_id"));
        item.setFlavourRangeId(rs.getInt("flavour_range_id"));
        item.setForecastInd(rs.getString("forecast_ind"));
        item.setFreeRangeId(rs.getString("free_range_id"));
        item.setFromTemp(rs.getInt("from_temp"));
        item.setGroupId(rs.getInt("group_id"));
        item.setHighMaxTemp(rs.getInt("high_max_temp"));
        item.setHighMinTemp(rs.getInt("high_min_temp"));
        item.setItemGrandparent(rs.getString("item_grandparent"));
        item.setItemParent(rs.getString("item_parent"));
        item.setKidzId(rs.getString("kidz_id"));
        item.setOrderableInd(rs.getString("orderable_ind"));
        item.setPackInd(rs.getString("pack_ind"));
        item.setPackMember(rs.getString("pack_member"));
        item.setPackQty(rs.getBigDecimal("pack_qty"));
        item.setPhaseId(rs.getInt("phase_id"));
        item.setPriceMarkInd(rs.getString("price_mark_ind"));
        item.setPrimaryRefItemInd(rs.getString("primary_ref_item_ind"));
        item.setPrimarySizeDsc(rs.getString("primary_size_dsc"));
        item.setPrimarySizeGroupId(rs.getString("primary_size_group_id"));
        item.setPrimarySizeId(rs.getString("primary_size_id"));
        item.setPrimarySizeRangeId(rs.getInt("primary_size_range_id"));
        item.setProductGroupScaling(rs.getString("product_group_scaling"));
        item.setProductId(rs.getString("product_id"));
        item.setReferenceItemInd(rs.getString("reference_item_ind"));
        item.setScentDsc(rs.getString("scent_dsc"));
        item.setScentGroupId(rs.getString("scent_group_id"));
        item.setScentId(rs.getString("scent_id"));
        item.setScentRangeId(rs.getInt("scent_range_id"));
        item.setSeasonId(rs.getInt("season_id"));
        item.setSecondarySizeDsc(rs.getString("secondary_size_dsc"));
        item.setSecondarySizeGroupId(rs.getString("secondary_size_group_id"));
        item.setSecondarySizeId(rs.getString("secondary_size_id"));
        item.setSecondarySizeRangeId(rs.getInt("secondary_size_range_id"));
        item.setSellableInd(rs.getString("sellable_ind"));
        item.setShortDsc(rs.getString("short_dsc"));
        item.setSimplePackInd(rs.getString("simple_pack_ind"));
        item.setSizeProfileInd(rs.getString("size_profile_ind"));
        item.setStandardUom(rs.getString("standard_uom"));
        item.setStatus(rs.getString("status"));
        item.setSubGroupId(rs.getInt("sub_group_id"));
        item.setSubclassId(rs.getInt("subclass_id"));
        item.setSupplierNo(rs.getInt("supplier_no"));
        item.setToTemp(rs.getInt("to_temp"));
        item.setTranInd(rs.getString("tran_ind"));
        item.setTranLevel(rs.getInt("tran_level"));
        item.setWwColour(rs.getString("ww_colour"));
        item.setWwSize(rs.getString("ww_size"));
        item.setWwStaticMass(rs.getBigDecimal("ww_static_mass"));
        item.setWwStyle(rs.getString("ww_style"));
        item.setWwStyleColour(rs.getString("ww_style_colour"));

        item.setVariableWeightInd(rs.getString("variable_weight_ind") != null ? rs.getString("variable_weight_ind").charAt(0) : null);
        item.setLooseProdInd(rs.getString("loose_prod_ind") != null ? rs.getString("loose_prod_ind").charAt(0) : null);
        item.setItemScaleInd(rs.getString("item_scale_ind") != null ? rs.getString("item_scale_ind").charAt(0) : null);
        item.setLegacySkuNo(rs.getString("legacy_sku_no"));
        item.setLegacyRandomMassInd(rs.getString("legacy_random_mass_ind") != null ? rs.getString("legacy_random_mass_ind").charAt(0) : null);
        item.setLegacyVatInd(rs.getString("legacy_vat_ind") != null ? rs.getString("legacy_vat_ind").charAt(0) : null);
        item.setActionInd(rs.getString("action_ind") != null ? rs.getString("action_ind").charAt(0) : null);

        item.setExtractSeqNo(rs.getLong("extract_seq_no"));
        item.setVatCde(rs.getString("vat_cde"));
        item.setVatRate(rs.getBigDecimal("vat_rate"));
        item.setSourceSystem(rs.getString("source_system"));
        item.setVpnNo(rs.getString("vpn_no"));
        item.setExtRefNo(rs.getString("ext_ref_no"));
        item.setItemLongDesc(rs.getString("item_long_desc"));
        item.setSegregationInd(rs.getString("segregation_ind"));
        item.setProdClass(rs.getString("prod_class"));

        // Return the populated Item object
        return item;
    }

    // Custom Serializable MapFunction for JSON conversion
    public static class ItemToJsonFunction implements MapFunction<Item, String>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;

        private transient Gson gson;

        @Override
        public String map(Item item)
        {
            return gson.toJson(item);
        }
    }
}
