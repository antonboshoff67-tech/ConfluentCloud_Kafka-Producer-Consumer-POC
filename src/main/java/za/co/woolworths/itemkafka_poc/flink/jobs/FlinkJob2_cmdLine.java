package za.co.woolworths.itemkafka_poc.flink.jobs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import za.co.woolworths.itemkafka_poc.kafka.consumer.LocalDateTimeAdapter;
import za.co.woolworths.itemkafka_poc.model.Item;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
public class FlinkJob2_cmdLine
{
   private String bootstrapServers;
   private String mysqlJdbcUrl;

   private String mysqlUsername;

   private String mysqlPassword;

   private static final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDateTime.class, new LocalDateTimeAdapter())
            .serializeNulls()          // This will serialize fields even with null values
            .create();

   private static final String INSERT_QUERY =
            "INSERT INTO Item (item_id, item_level, item_number_type, prefix, allocator_system, " +
                    "business_unit_id, catch_weight_ind, class_id, colour_dsc, colour_group_id, colour_id, colour_range_id, company_id, " +
                    "count_on_us_id, create_dte, dept_id, discipline, domain_id, flavour_dsc, flavour_group_id, " +
                    "flavour_id, flavour_range_id, forecast_ind, free_range_id, from_temp, group_id, high_max_temp, " +
                    "high_min_temp, item_grandparent, item_parent, kidz_id, orderable_ind, pack_ind, pack_member, pack_qty, " +
                    "phase_id, price_mark_ind, primary_ref_item_ind, primary_size_dsc, primary_size_group_id, primary_size_id, " +
                    "primary_size_range_id, product_group_scaling, product_id, reference_item_ind, scent_dsc, scent_group_id, " +
                    "scent_id, scent_range_id, season_id, secondary_size_dsc, secondary_size_group_id, secondary_size_id, " +
                    "secondary_size_range_id, sellable_ind, short_dsc, simple_pack_ind, size_profile_ind, standard_uom, " +
                    "status, sub_group_id, subclass_id, supplier_no, to_temp, tran_ind, tran_level, ww_colour, ww_size, " +
                    "ww_static_mass, ww_style, ww_style_colour, variable_weight_ind, loose_prod_ind, item_scale_ind, " +
                    "legacy_sku_no, legacy_random_mass_ind, legacy_vat_ind, action_ind, extract_seq_no, vat_cde, vat_rate, " +
                    "source_system, vpn_no, ext_ref_no, item_long_desc, segregation_ind, prod_class, last_update_dte) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                    "ON DUPLICATE KEY UPDATE item_level = VALUES(item_level), item_number_type = VALUES(item_number_type), " +
                    "prefix = VALUES(prefix), allocator_system = VALUES(allocator_system), business_unit_id = VALUES(business_unit_id), " +
                    "catch_weight_ind = VALUES(catch_weight_ind), class_id = VALUES(class_id), colour_dsc = VALUES(colour_dsc), " +
                    "colour_group_id = VALUES(colour_group_id), colour_id = VALUES(colour_id), colour_range_id = VALUES(colour_range_id), " +
                    "company_id = VALUES(company_id), count_on_us_id = VALUES(count_on_us_id), create_dte = VALUES(create_dte), " +
                    "dept_id = VALUES(dept_id), discipline = VALUES(discipline), domain_id = VALUES(domain_id), flavour_dsc = VALUES(flavour_dsc), " +
                    "flavour_group_id = VALUES(flavour_group_id), flavour_id = VALUES(flavour_id), flavour_range_id = VALUES(flavour_range_id), " +
                    "forecast_ind = VALUES(forecast_ind), free_range_id = VALUES(free_range_id), from_temp = VALUES(from_temp), " +
                    "group_id = VALUES(group_id), high_max_temp = VALUES(high_max_temp), high_min_temp = VALUES(high_min_temp), " +
                    "item_grandparent = VALUES(item_grandparent), item_parent = VALUES(item_parent), kidz_id = VALUES(kidz_id), " +
                    "orderable_ind = VALUES(orderable_ind), pack_ind = VALUES(pack_ind), pack_member = VALUES(pack_member), " +
                    "pack_qty = VALUES(pack_qty), phase_id = VALUES(phase_id), price_mark_ind = VALUES(price_mark_ind), " +
                    "primary_ref_item_ind = VALUES(primary_ref_item_ind), primary_size_dsc = VALUES(primary_size_dsc), " +
                    "primary_size_group_id = VALUES(primary_size_group_id), primary_size_id = VALUES(primary_size_id), " +
                    "primary_size_range_id = VALUES(primary_size_range_id), product_group_scaling = VALUES(product_group_scaling), " +
                    "product_id = VALUES(product_id), reference_item_ind = VALUES(reference_item_ind), scent_dsc = VALUES(scent_dsc), " +
                    "scent_group_id = VALUES(scent_group_id), scent_id = VALUES(scent_id), scent_range_id = VALUES(scent_range_id), " +
                    "season_id = VALUES(season_id), secondary_size_dsc = VALUES(secondary_size_dsc), secondary_size_group_id = VALUES(secondary_size_group_id), secondary_size_id = VALUES(secondary_size_id), secondary_size_range_id = VALUES(secondary_size_range_id), sellable_ind = VALUES(sellable_ind), short_dsc = VALUES(short_dsc), simple_pack_ind = VALUES(simple_pack_ind), size_profile_ind = VALUES(size_profile_ind), standard_uom = VALUES(standard_uom), status = VALUES(status), sub_group_id = VALUES(sub_group_id), subclass_id = VALUES(subclass_id), supplier_no = VALUES(supplier_no), to_temp = VALUES(to_temp), tran_ind = VALUES(tran_ind), tran_level = VALUES(tran_level), ww_colour = VALUES(ww_colour), ww_size = VALUES(ww_size), ww_static_mass = VALUES(ww_static_mass), ww_style = VALUES(ww_style), ww_style_colour = VALUES(ww_style_colour), variable_weight_ind = VALUES(variable_weight_ind), loose_prod_ind = VALUES(loose_prod_ind), item_scale_ind = VALUES(item_scale_ind), legacy_sku_no = VALUES(legacy_sku_no), legacy_random_mass_ind = VALUES(legacy_random_mass_ind), legacy_vat_ind = VALUES(legacy_vat_ind), action_ind = VALUES(action_ind), extract_seq_no = VALUES(extract_seq_no), vat_cde = VALUES(vat_cde), vat_rate = VALUES(vat_rate), source_system = VALUES(source_system), vpn_no = VALUES(vpn_no), ext_ref_no = VALUES(ext_ref_no), item_long_desc = VALUES(item_long_desc), segregation_ind = VALUES(segregation_ind), prod_class = VALUES(prod_class), last_update_dte = VALUES(last_update_dte)";

    public FlinkJob2_cmdLine(String bootstrapServers, String mysqlJdbcUrl, String mysqlUsername, String mysqlPassword)
    {
        this.mysqlJdbcUrl = mysqlJdbcUrl;
        this.bootstrapServers = bootstrapServers;
        this.mysqlUsername = mysqlUsername;
        this.mysqlPassword = mysqlPassword;
    }

   public void run()
   {
        try
        {
            // Initialize the Flink execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            log.debug("Flink Job 2: Starting to consume from Kafka.");

            /// Set up checkpointing with a restart strategy
            // Enable checkpointing
           /* env.enableCheckpointing(10000); // Set checkpoint interval to 10 seconds
            // Configure checkpointing for exactly-once semantics
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.convertFromCheckpointingMode(CheckpointingMode.EXACTLY_ONCE));
            checkpointConfig.setMinPauseBetweenCheckpoints(500); // Minimum pause between checkpoints
            checkpointConfig.setTolerableCheckpointFailureNumber(1); // Allow one checkpoint failure*/

            // Set up the Kafka source
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setTopics("Item_Topic")
                    //.setGroupId("item_group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // Create a DataStream from the Kafka source
            DataStream<String> messageStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            // Map the messages to Item objects and sink to MySQL
            messageStream
                    //.map(new ItemFromJsonFunction(gson))
                    .map(new ItemFromJsonFunction())
                    .filter(item -> item != null)  // Filter out null items
                    .addSink(JdbcSink.sink(
                            // INSERT_QUERY variable here...
                            INSERT_QUERY,
                            (ps, item) ->
                            {
                                try
                                {
                                    // Set parameters for the item to insert into the database
                                    ps.setString(1, item.getItemId());
                                    ps.setInt(2, item.getItemLevel());
                                    ps.setString(3, item.getItemNumberType());
                                    ps.setInt(4, item.getPrefix());
                                    ps.setString(5, item.getAllocatorSystem());
                                    ps.setInt(6, item.getBusinessUnitId());
                                    ps.setString(7, item.getCatchWeightInd());
                                    ps.setInt(8, item.getClassId());
                                    ps.setString(9, item.getColourDsc());
                                    ps.setString(10, item.getColourGroupId());
                                    ps.setString(11, item.getColourId());
                                    ps.setInt(12, item.getColourRangeId());
                                    ps.setInt(13, item.getCompanyId());
                                    ps.setString(14, item.getCountOnUsId());
                                    ps.setObject(15, item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now()); // LocalDateTime for create_dte
                                    ps.setInt(16, item.getDeptId());
                                    ps.setString(17, item.getDiscipline());
                                    ps.setInt(18, item.getDomainId());
                                    ps.setString(19, item.getFlavourDsc());
                                    ps.setString(20, item.getFlavourGroupId());
                                    ps.setString(21, item.getFlavourId());
                                    ps.setInt(22, item.getFlavourRangeId());
                                    ps.setString(23, item.getForecastInd());
                                    ps.setString(24, item.getFreeRangeId());
                                    ps.setInt(25, item.getFromTemp());
                                    ps.setInt(26, item.getGroupId());
                                    ps.setInt(27, item.getHighMaxTemp());
                                    ps.setInt(28, item.getHighMinTemp());
                                    ps.setString(29, item.getItemGrandparent());
                                    ps.setString(30, item.getItemParent());
                                    ps.setString(31, item.getKidzId());
                                    ps.setString(32, item.getOrderableInd());
                                    ps.setString(33, item.getPackInd());
                                    ps.setString(34, item.getPackMember());
                                    ps.setBigDecimal(35, item.getPackQty());
                                    ps.setInt(36, item.getPhaseId());
                                    ps.setString(37, item.getPriceMarkInd());
                                    ps.setString(38, item.getPrimaryRefItemInd());
                                    ps.setString(39, item.getPrimarySizeDsc());
                                    ps.setString(40, item.getPrimarySizeGroupId());
                                    ps.setString(41, item.getPrimarySizeId());
                                    ps.setInt(42, item.getPrimarySizeRangeId());
                                    ps.setString(43, item.getProductGroupScaling());
                                    ps.setString(44, item.getProductId());
                                    ps.setString(45, item.getReferenceItemInd());
                                    ps.setString(46, item.getScentDsc());
                                    ps.setString(47, item.getScentGroupId());
                                    ps.setString(48, item.getScentId());
                                    ps.setInt(49, item.getScentRangeId());
                                    ps.setInt(50, item.getSeasonId());
                                    ps.setString(51, item.getSecondarySizeDsc());
                                    ps.setString(52, item.getSecondarySizeGroupId());
                                    ps.setString(53, item.getSecondarySizeId());
                                    ps.setInt(54, item.getSecondarySizeRangeId());
                                    ps.setString(55, item.getSellableInd());
                                    ps.setString(56, item.getShortDsc());
                                    ps.setString(57, item.getSimplePackInd());
                                    ps.setString(58, item.getSizeProfileInd());
                                    ps.setString(59, item.getStandardUom());
                                    ps.setString(60, item.getStatus());
                                    ps.setInt(61, item.getSubGroupId());
                                    ps.setInt(62, item.getSubclassId());
                                    ps.setInt(63, item.getSupplierNo());
                                    ps.setInt(64, item.getToTemp());
                                    ps.setString(65, item.getTranInd());
                                    ps.setInt(66, item.getTranLevel());
                                    ps.setString(67, item.getWwColour());
                                    ps.setString(68, item.getWwSize());
                                    ps.setBigDecimal(69, item.getWwStaticMass());
                                    ps.setString(70, item.getWwStyle());
                                    ps.setString(71, item.getWwStyleColour());
                                    log.debug("Inserting item with VariableWeightInd: {}", String.valueOf(item.getVariableWeightInd()));
                                    ps.setString(72, String.valueOf(item.getVariableWeightInd())); // Character fields
                                    ps.setString(73, String.valueOf(item.getLooseProdInd()));
                                    ps.setString(74, String.valueOf(item.getItemScaleInd()));
                                    ps.setString(75, item.getLegacySkuNo());
                                    ps.setString(76, String.valueOf(item.getLegacyRandomMassInd()));
                                    ps.setString(77, String.valueOf(item.getLegacyVatInd()));
                                    ps.setString(78, String.valueOf(item.getActionInd()));
                                    ps.setLong(79, item.getExtractSeqNo());
                                    ps.setString(80, item.getVatCde());
                                    ps.setBigDecimal(81, item.getVatRate());
                                    ps.setString(82, item.getSourceSystem());
                                    ps.setString(83, item.getVpnNo());
                                    ps.setString(84, item.getExtRefNo());
                                    ps.setString(85, item.getItemLongDesc());
                                    ps.setString(86, item.getSegregationInd());
                                    ps.setString(87, item.getProdClass());
                                    ps.setObject(88, item.getLastUpdateDte()); // Use setObject for LocalDateTime
                                }
                                catch (Exception e)
                                {
                                    log.error("Error preparing statement for item {}: {}", item.getItemId(), e.getMessage(), e);
                                }
                            },
                            JdbcExecutionOptions.builder()
                                    .withBatchSize(1000) // Number of records to write in one batch
                                    .withBatchIntervalMs(200) // Time interval for batching
                                    .withMaxRetries(3) // Number of retries in case of failures
                                    .build(),
                            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl(mysqlJdbcUrl) // Your MySQL connection string
                                    .withDriverName("com.mysql.cj.jdbc.Driver") // MySQL driver
                                    .withUsername(mysqlUsername) // Your MySQL username
                                    .withPassword(mysqlPassword) // Your MySQL password
                                    .build()
                    ));

            log.debug("Flink job is set up to read from Kafka and write to MySQL.");

            // Execute the job
            log.debug("Executing the Flink job now ... using env.execute ...");
            try
            {
                env.execute("Flink Job 2: Consume Kafka Messages for Logging");
            }
            catch (Exception e)
            {
                log.error("Flink Job 2 failed: {}", e.getMessage(), e); // Handle exceptions
            }
            log.debug("Flink job2 completed...");

        }
        catch (Exception e)
        {
            log.error("Flink Job 2 failed: {}", e.getMessage(), e); // Handle exceptions
        }
    }

    // ItemFromJsonFunction is responsible for mapping JSON to Item objects
    public static class ItemFromJsonFunction implements MapFunction<String, Item>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;
        private int itemCounter = 0;

        @Override
        public Item map(String json)
        {
            try
            {
                itemCounter++;
                log.debug("Received JSON item nr " + itemCounter + " from Kafka: {}", json);
                Item item = gson.fromJson(json, Item.class);
                if (item != null)
                {
                    // cater for null values everywhere
                    item.setItemId(item.getItemId() != null ? item.getItemId() : UUID.randomUUID().toString());
                    item.setItemNumberType(item.getItemNumberType() != null ? item.getItemNumberType() : "UNKNOWN");
                    item.setBusinessUnitId(item.getBusinessUnitId() != null ? item.getBusinessUnitId() : 0);
                    item.setCreateDte(item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                    // Ensure date conversion if Gson doesn't parse it correctly
                    if (item.getCreateDte() == null && json.contains("createDte"))
                    {
                        LocalDateTime parsedDate = LocalDateTime.parse(json.split("\"createDte\":\"")[1].split("\"")[0]);
                        item.setCreateDte(parsedDate);
                    }

                    item.setCreateDte(item.getCreateDte() != null ? item.getCreateDte() : LocalDateTime.now());
                    item.setItemLevel(item.getItemLevel() != null ? item.getItemLevel() : 0);
                    item.setVatRate(item.getVatRate() != null ? item.getVatRate() : BigDecimal.ZERO);
                    item.setStandardUom(item.getStandardUom() != null ? item.getStandardUom() : "EA");
                    item.setPrefix(item.getPrefix() != null ? item.getPrefix() : 0);
                    item.setAllocatorSystem(item.getAllocatorSystem() != null ? item.getAllocatorSystem() : "DEFAULT");
                    item.setCatchWeightInd(item.getCatchWeightInd() != null ? item.getCatchWeightInd() : "N");
                    item.setClassId(item.getClassId() != null ? item.getClassId() : 0);
                    item.setColourGroupId(item.getColourGroupId() != null ? item.getColourGroupId() : "None");
                    item.setColourId(item.getColourId() != null ? item.getColourId() : "UNKNOWN");
                    item.setColourRangeId(item.getColourRangeId() != null ? item.getColourRangeId() : 0);
                    item.setCompanyId(item.getCompanyId() != null ? item.getCompanyId() : 0);
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
                    item.setProdClass(item.getProdClass() != null ? item.getProdClass() : "N");
                    // Repeat for other LocalDateTime fields
                    item.setLastUpdateDte(item.getLastUpdateDte() != null ? item.getLastUpdateDte() : LocalDateTime.now());
                    if (item.getLastUpdateDte() == null && json.contains("lastUpdateDte")) {
                        LocalDateTime parsedDate = LocalDateTime.parse(json.split("\"lastUpdateDte\":\"")[1].split("\"")[0]);
                        item.setLastUpdateDte(parsedDate);
                    }
                    item.setLastUpdateDte(item.getLastUpdateDte() != null ? item.getLastUpdateDte() : LocalDateTime.now());
                }
                else
                {
                    log.error("***Item pulled from Kafka Topic was null ... could not be Deserialized .. item was: {}", json);
                }
                log.debug("***Item serialized successfully was ....: {}", item);
                return item;
            }
            catch (Exception e)
            {
                log.error("Deserialization failed for JSON: {}", json, e);
                return null; // Return null if deserialization fails
            }
        }
   }

}
