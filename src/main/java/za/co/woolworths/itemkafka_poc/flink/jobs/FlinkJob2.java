package za.co.woolworths.itemkafka_poc.flink.jobs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j; // Use Lombok's Slf4j for logging
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import za.co.woolworths.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import za.co.woolworths.itemkafka_poc.model.Item;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@Slf4j // Using Lombok's Slf4j annotation for logging
public class FlinkJob2
{
    public static void main(String[] args) throws Exception
    {
        try
        {
            // At the start of the job
            log.debug("Starting Flink Job 2: Kafka to MySQL.");
            // Use the existing execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*// Creating a Configuration object to specify Flink settings
            Configuration flinkConfig = new Configuration();
            flinkConfig.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), "1024m"); // Total TaskManager memory
            flinkConfig.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "512m"); // Managed memory for tasks
            flinkConfig.set(TaskManagerOptions.NUM_TASK_SLOTS, 2); // Set number of task slots*/
            /*// Apply configuration to the environment
            env.getConfig().setGlobalJobParameters(flinkConfig);*/
            // Create a Gson instance for JSON serialization/deserialization

            // Kafka Source configuration logging
            log.debug("Setting up Kafka source configuration with topic: 'Item_Topic'.");
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("Item_Topic")
                    .setGroupId("item_group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize strings only
                    .build();

            log.debug("Attempting to read from Kafka topic: {}", "Item_Topic");
            Gson gson = new GsonBuilder()
                    .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
                    .create();

            // Transform stream and ensure immediate processing with proper order
            DataStream<Item> itemStream = env
                    .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                    .map(new ItemFromJsonFunction(gson))
                    .filter(Objects::nonNull)  // Filter out nulls resulting from deserialization errors
                    .map(item -> {
                        // Make sure the item is not null or missing necessary attributes
                        if (item.getItemId() == null) {
                            item.setItemId(UUID.randomUUID().toString());
                        }
                        return item;
                    });

            // Before sink to MySQL
            log.debug("Initializing MySQL sink function..."); // ###
            // Sink: Write to MySQL database
            itemStream.addSink(JdbcSink.sink(
                    "INSERT INTO ITEM (item_id, item_level, item_number_type, ...) VALUES (?, ?, ...)", // Adjust with all fields
                    (ps, item) ->
                    {
                        try
                        {
                            ps.setString(1, item.getItemId() != null ? item.getItemId() : UUID.randomUUID().toString());
                            ps.setInt(2, item.getItemLevel() != null ? item.getItemLevel() : 0);
                            ps.setString(3, item.getItemNumberType() != null ? item.getItemNumberType() : "UNKNOWN");
                            ps.setInt(4, item.getPrefix() != null ? item.getPrefix() : 0);
                            ps.setString(5, item.getAllocatorSystem() != null ? item.getAllocatorSystem() : "DEFAULT");
                            ps.setInt(6, item.getBusinessUnitId() != null ? item.getBusinessUnitId() : 0);
                            ps.setString(7, item.getCatchWeightInd() != null ? item.getCatchWeightInd() : "N");
                            ps.setInt(8, item.getClassId() != null ? item.getClassId() : 0);
                            ps.setString(9, item.getColourDsc() != null ? item.getColourDsc() : "UNKNOWN");
                            ps.setString(10, item.getColourGroupId() != null ? item.getColourGroupId() : UUID.randomUUID().toString());
                            ps.setString(11, item.getColourId() != null ? item.getColourId() : "UNKNOWN");
                            ps.setInt(12, item.getColourRangeId() != null ? item.getColourRangeId() : 0);
                            ps.setInt(13, item.getCompanyId() != null ? item.getCompanyId() : 0);
                            ps.setObject(14, item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                            ps.setInt(15, item.getDeptId() != null ? item.getDeptId() : 0);
                            ps.setString(16, item.getDiscipline() != null ? item.getDiscipline() : "UNKNOWN");
                            ps.setInt(17, item.getDomainId() != null ? item.getDomainId() : 0);
                            ps.setString(18, item.getFlavourDsc() != null ? item.getFlavourDsc() : "UNKNOWN");
                            ps.setString(19, item.getFlavourGroupId() != null ? item.getFlavourGroupId() : "UNKNOWN");
                            ps.setString(20, item.getFlavourId() != null ? item.getFlavourId() : "UNKNOWN");
                            ps.setInt(21, item.getFlavourRangeId() != null ? item.getFlavourRangeId() : 0);
                            ps.setString(22, item.getForecastInd() != null ? item.getForecastInd() : "N");
                            ps.setString(23, item.getFreeRangeId() != null ? item.getFreeRangeId() : "UNKNOWN");
                            ps.setInt(24, item.getFromTemp() != null ? item.getFromTemp() : 0);
                            ps.setInt(25, item.getGroupId() != null ? item.getGroupId() : 0);
                            ps.setInt(26, item.getHighMaxTemp() != null ? item.getHighMaxTemp() : 0);
                            ps.setInt(27, item.getHighMinTemp() != null ? item.getHighMinTemp() : 0);
                            ps.setString(28, item.getItemGrandparent() != null ? item.getItemGrandparent() : "UNKNOWN");
                            ps.setString(29, item.getItemParent() != null ? item.getItemParent() : "UNKNOWN");
                            ps.setString(30, item.getKidzId() != null ? item.getKidzId() : "UNKNOWN");
                            ps.setString(31, item.getOrderableInd() != null ? item.getOrderableInd() : "N");
                            ps.setString(32, item.getPackInd() != null ? item.getPackInd() : "N");
                            ps.setString(33, item.getPackMember() != null ? item.getPackMember() : "UNKNOWN");
                            ps.setBigDecimal(34, item.getPackQty() != null ? item.getPackQty() : BigDecimal.ZERO);
                            ps.setInt(35, item.getPhaseId() != null ? item.getPhaseId() : 0);
                            ps.setString(36, item.getPriceMarkInd() != null ? item.getPriceMarkInd() : "N");
                            ps.setString(37, item.getPrimaryRefItemInd() != null ? item.getPrimaryRefItemInd() : "N");
                            ps.setString(38, item.getPrimarySizeDsc() != null ? item.getPrimarySizeDsc() : "UNKNOWN");
                            ps.setString(39, item.getPrimarySizeGroupId() != null ? item.getPrimarySizeGroupId() : "UNKNOWN");
                            ps.setString(40, item.getPrimarySizeId() != null ? item.getPrimarySizeId() : "UNKNOWN");
                            ps.setInt(41, item.getPrimarySizeRangeId() != null ? item.getPrimarySizeRangeId() : 0);
                            ps.setString(42, item.getProductGroupScaling() != null ? item.getProductGroupScaling() : "UNKNOWN");
                            ps.setString(43, item.getProductId() != null ? item.getProductId() : "UNKNOWN");
                            ps.setString(44, item.getReferenceItemInd() != null ? item.getReferenceItemInd() : "N");
                            ps.setString(45, item.getScentDsc() != null ? item.getScentDsc() : "UNKNOWN");
                            ps.setString(46, item.getScentGroupId() != null ? item.getScentGroupId() : "UNKNOWN");
                            ps.setString(47, item.getScentId() != null ? item.getScentId() : "UNKNOWN");
                            ps.setInt(48, item.getScentRangeId() != null ? item.getScentRangeId() : 0);
                            ps.setInt(49, item.getSeasonId() != null ? item.getSeasonId() : 0);
                            ps.setString(50, item.getSecondarySizeDsc() != null ? item.getSecondarySizeDsc() : "UNKNOWN");
                            ps.setString(51, item.getSecondarySizeGroupId() != null ? item.getSecondarySizeGroupId() : "UNKNOWN");
                            ps.setString(52, item.getSecondarySizeId() != null ? item.getSecondarySizeId() : "UNKNOWN");
                            ps.setInt(53, item.getSecondarySizeRangeId() != null ? item.getSecondarySizeRangeId() : 0);
                            ps.setString(54, item.getSellableInd() != null ? item.getSellableInd() : "N");
                            ps.setString(55, item.getShortDsc() != null ? item.getShortDsc() : "N/A");
                            ps.setString(56, item.getSimplePackInd() != null ? item.getSimplePackInd() : "N");
                            ps.setString(57, item.getSizeProfileInd() != null ? item.getSizeProfileInd() : "N");
                            ps.setString(58, item.getStandardUom() != null ? item.getStandardUom() : "EA");
                            ps.setString(59, item.getStatus() != null ? item.getStatus() : "A");
                            ps.setInt(60, item.getSubGroupId() != null ? item.getSubGroupId() : 0);
                            ps.setInt(61, item.getSubclassId() != null ? item.getSubclassId() : 0);
                            ps.setInt(62, item.getSupplierNo() != null ? item.getSupplierNo() : 0);
                            ps.setInt(63, item.getToTemp() != null ? item.getToTemp() : 0);
                            ps.setString(64, item.getTranInd() != null ? item.getTranInd() : "N");
                            ps.setInt(65, item.getTranLevel() != null ? item.getTranLevel() : 0);
                            ps.setString(66, item.getWwColour() != null ? item.getWwColour() : "UNKNOWN");
                            ps.setString(67, item.getWwSize() != null ? item.getWwSize() : "UNKNOWN");
                            ps.setBigDecimal(68, item.getWwStaticMass() != null ? item.getWwStaticMass() : BigDecimal.ZERO);
                            ps.setString(69, item.getWwStyle() != null ? item.getWwStyle() : "UNKNOWN");
                            ps.setString(70, item.getWwStyleColour() != null ? item.getWwStyleColour() : "UNKNOWN");
                            ps.setString(71, item.getVariableWeightInd() != null ? item.getVariableWeightInd().toString() : "N");
                            ps.setString(72, item.getLooseProdInd() != null ? item.getLooseProdInd().toString() : "N");
                            ps.setString(73, item.getItemScaleInd() != null ? item.getItemScaleInd().toString() : "N");
                            ps.setString(74, item.getLegacySkuNo() != null ? item.getLegacySkuNo() : "N/A");
                            ps.setString(75, item.getLegacyRandomMassInd() != null ? item.getLegacyRandomMassInd().toString() : "N");
                            ps.setString(76, item.getLegacyVatInd() != null ? item.getLegacyVatInd().toString() : "N");
                            ps.setString(77, item.getActionInd() != null ? item.getActionInd().toString() : "N");
                            ps.setLong(78, item.getExtractSeqNo() != null ? item.getExtractSeqNo() : 0L);
                            ps.setString(79, item.getVatCde() != null ? item.getVatCde() : "S");
                            ps.setBigDecimal(80, item.getVatRate() != null ? item.getVatRate() : BigDecimal.ZERO);
                            ps.setString(81, item.getSourceSystem() != null ? item.getSourceSystem() : "UNKNOWN");
                            ps.setString(82, item.getVpnNo() != null ? item.getVpnNo() : "N/A");
                            ps.setString(83, item.getExtRefNo() != null ? item.getExtRefNo() : "N/A");
                            ps.setString(84, item.getItemLongDesc() != null ? item.getItemLongDesc() : "No Description");
                            ps.setString(85, item.getSegregationInd() != null ? item.getSegregationInd() : "N");
                            ps.setString(86, item.getProdClass() != null ? item.getProdClass() : "N/A");
                            ps.setObject(87, item.getLastUpdateDte() != null ? item.getLastUpdateDte() : LocalDateTime.now());
                        }
                        catch (SQLException e)
                        {
                            log.error("Error inserting item into database: {}", e.getMessage());
                        }
                    },
                    JdbcExecutionOptions.builder()
                            .withBatchSize(1000) // Number of records to write in one batch
                            .withBatchIntervalMs(200) // Time interval for batching
                            .withMaxRetries(3) // Number of retries in case of failures
                            .build(),
                    new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                            .withUrl("jdbc:mysql://localhost:3306/cs_caissa_central_master_data?useSSL=false&allowPublicKeyRetrieval=true") // MySQL connection string
                            .withDriverName("com.mysql.cj.jdbc.Driver") // MySQL driver
                            .withUsername("root") // MySQL username
                            .withPassword("Anton@Perfect07") // MySQL password
                            .build()
            ));

            log.debug("Invoking env.execute function to execute flink job 2 ..."); // ###
            // Execute the Flink job
            env.execute("Flink Job 2: Kafka to MySQL");
            log.debug("Flink Job 2 executed successfully..."); // ###
        }
        catch (Exception e)
        {
            log.error("Execution failed for Flink Job 2: {}", e.getMessage(), e); // Handle exceptions
        }
    }

    public static class ItemFromJsonFunction implements MapFunction<String, Item>, java.io.Serializable {
        private static final long serialVersionUID = 1L;
        private transient Gson gson;

        public ItemFromJsonFunction(Gson gson)
        {
            this.gson = gson;
        }

        @Override
        public Item map(String json)
        {
            try
            {
                log.debug("Received JSON from Kafka: {}", json);
                Item item = gson.fromJson(json, Item.class);
                if (item != null)
                {
                    item.setItemId(item.getItemId() != null ? item.getItemId() : UUID.randomUUID().toString());
                    item.setItemNumberType(item.getItemNumberType() != null ? item.getItemNumberType() : "UNKNOWN");
                    item.setBusinessUnitId(item.getBusinessUnitId() != null ? item.getBusinessUnitId() : 0);
                    item.setCreateDte(item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                    item.setItemLevel(item.getItemLevel() != null ? item.getItemLevel() : 0);
                    item.setVatRate(item.getVatRate() != null ? item.getVatRate() : BigDecimal.ZERO);
                    item.setStandardUom(item.getStandardUom() != null ? item.getStandardUom() : "EA");
                    item.setPrefix(item.getPrefix() != null ? item.getPrefix() : 0);
                    item.setAllocatorSystem(item.getAllocatorSystem() != null ? item.getAllocatorSystem() : "DEFAULT");
                    item.setCatchWeightInd(item.getCatchWeightInd() != null ? item.getCatchWeightInd() : "N");
                    item.setClassId(item.getClassId() != null ? item.getClassId() : 0);
                    item.setColourGroupId(item.getColourGroupId() != null ? item.getColourGroupId() : UUID.randomUUID().toString());
                    item.setColourId(item.getColourId() != null ? item.getColourId() : "UNKNOWN");
                    item.setColourRangeId(item.getColourRangeId() != null ? item.getColourRangeId() : 0);
                    item.setCompanyId(item.getCompanyId() != null ? item.getCompanyId() : 0);
                    item.setCreateDte(item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                    item.setDeptId(item.getDeptId() != null ? item.getDeptId() : 0);
                    item.setDomainId(item.getDomainId() != null ? item.getDomainId() : 0);
                    item.setFlavourRangeId(item.getFlavourRangeId() != null ? item.getFlavourRangeId() : 0);
                    item.setForecastInd(item.getForecastInd() != null ? item.getForecastInd() : "N");
                    item.setFromTemp(item.getFromTemp() != null ? item.getFromTemp() : 0);
                    item.setGroupId(item.getGroupId() != null ? item.getGroupId() : 0);
                    item.setHighMaxTemp(item.getHighMaxTemp() != null ? item.getHighMaxTemp() : 0);
                    item.setHighMinTemp(item.getHighMinTemp() != null ? item.getHighMinTemp() : 0);
                    item.setOrderableInd(item.getOrderableInd() != null ? item.getOrderableInd() : "N");
                    item.setPackInd(item.getPackInd() != null ? item.getPackInd() : "N");
                    item.setPhaseId(item.getPhaseId() != null ? item.getPhaseId() : 1);
                    item.setPriceMarkInd(item.getPriceMarkInd() != null ? item.getPriceMarkInd() : "N");
                    item.setPrimaryRefItemInd(item.getPrimaryRefItemInd() != null ? item.getPrimaryRefItemInd() : "N");
                    item.setPrimarySizeRangeId(item.getPrimarySizeRangeId() != null ? item.getPrimarySizeRangeId() : 0);
                    item.setReferenceItemInd(item.getReferenceItemInd() != null ? item.getReferenceItemInd() : "N");
                    item.setScentRangeId(item.getScentRangeId() != null ? item.getScentRangeId() : 0);
                    item.setSeasonId(item.getSeasonId() != null ? item.getSeasonId() : 0);
                    item.setSecondarySizeRangeId(item.getSecondarySizeRangeId() != null ? item.getSecondarySizeRangeId() : 0);
                    item.setSellableInd(item.getSellableInd() != null ? item.getSellableInd() : "N");
                    item.setShortDsc(item.getShortDsc() != null ? item.getShortDsc() : "N/A");
                    item.setSimplePackInd(item.getSimplePackInd() != null ? item.getSimplePackInd() : "N");
                    item.setSizeProfileInd(item.getSizeProfileInd() != null ? item.getSizeProfileInd() : "N");
                    item.setStandardUom(item.getStandardUom() != null ? item.getStandardUom() : "EA");
                    item.setStatus(item.getStatus() != null ? item.getStatus() : "A");
                    item.setSubGroupId(item.getSubGroupId() != null ? item.getSubGroupId() : 0);
                    item.setSubclassId(item.getSubclassId() != null ? item.getSubclassId() : 0);
                    item.setSupplierNo(item.getSupplierNo() != null ? item.getSupplierNo() : 0);
                    item.setToTemp(item.getToTemp() != null ? item.getToTemp() : 0);
                    item.setTranInd(item.getTranInd() != null ? item.getTranInd() : "N");
                    item.setTranLevel(item.getTranLevel() != null ? item.getTranLevel() : 0);
                    item.setWwColour(item.getWwColour() != null ? item.getWwColour() : "UNKNOWN");
                    item.setWwSize(item.getWwSize() != null ? item.getWwSize() : "UNKNOWN");
                    item.setWwStaticMass(item.getWwStaticMass() != null ? item.getWwStaticMass() : BigDecimal.ZERO);
                    item.setWwStyle(item.getWwStyle() != null ? item.getWwStyle() : UUID.randomUUID().toString());
                    item.setWwStyleColour(item.getWwStyleColour() != null ? item.getWwStyleColour() : "UNKNOWN");
                    item.setLegacySkuNo(item.getLegacySkuNo() != null ? item.getLegacySkuNo() : "N/A");
                    item.setVariableWeightInd(item.getVariableWeightInd() != null ? item.getVariableWeightInd() : 'N');
                    item.setLooseProdInd(item.getLooseProdInd() != null ? item.getLooseProdInd() : 'N');
                    item.setItemScaleInd(item.getItemScaleInd() != null ? item.getItemScaleInd() : 'N');
                    item.setLegacyRandomMassInd(item.getLegacyRandomMassInd() != null ? item.getLegacyRandomMassInd() : 'N');
                    item.setLegacyVatInd(item.getLegacyVatInd() != null ? item.getLegacyVatInd() : 'N');
                    item.setActionInd(item.getActionInd() != null ? item.getActionInd() : 'N');
                    item.setExtractSeqNo(item.getExtractSeqNo() != null ? item.getExtractSeqNo() : 0L);
                    item.setVatCde(item.getVatCde() != null ? item.getVatCde() : "S");
                    item.setVatRate(item.getVatRate() != null ? item.getVatRate() : BigDecimal.ZERO);
                    item.setSourceSystem(item.getSourceSystem() != null ? item.getSourceSystem() : "UNKNOWN");
                    item.setVpnNo(item.getVpnNo() != null ? item.getVpnNo() : "N/A");
                    item.setExtRefNo(item.getExtRefNo() != null ? item.getExtRefNo() : "N/A");
                    item.setItemLongDesc(item.getItemLongDesc() != null ? item.getItemLongDesc() : "No Description");
                    item.setSegregationInd(item.getSegregationInd() != null ? item.getSegregationInd() : "N");
                    item.setProdClass(item.getProdClass() != null ? item.getProdClass() : "N/A");
                    item.setLastUpdateDte(item.getLastUpdateDte() != null ? item.getLastUpdateDte() : LocalDateTime.now());
                }
                else
                {
                    log.error("Deserialization failed for JSON: {}", json);
                }
                return item;

            }
            catch (Exception e)
            {
                log.error("Deserialization failed for JSON: {}", json);
                return null; // Return null if deserialization fails
            }
        }
    }


}
