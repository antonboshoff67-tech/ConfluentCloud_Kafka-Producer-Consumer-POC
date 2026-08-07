package com.antontech.itemkafka_poc.model;

//import javax.persistence.*;
import com.google.gson.annotations.SerializedName;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "ITEM")
public class Item implements Serializable  {

    @Id
    @Column(name = "item_id", nullable = false) // This field is NOT NULL
    @SerializedName("itemId")
    @NotNull // Ensure the itemId is not null
    private String itemId;

    @Column(name = "item_level", nullable = true) // This field can be NULL
    @SerializedName("itemLevel")
    private Integer itemLevel;

    @Column(name = "item_number_type", nullable = true) // This field can be NULL
    @SerializedName("itemNumberType")
    private String itemNumberType;

    @Column(name = "prefix", nullable = true) // This field can be NULL
    @SerializedName("prefix")
    private Integer prefix;

    @Column(name = "allocator_system", nullable = true) // This field can be NULL
    @SerializedName("allocatorSystem")
    private String allocatorSystem;

    @Column(name = "business_unit_id", nullable = true) // This field can be NULL
    @SerializedName("businessUnitId")
    private Integer businessUnitId;

    @Column(name = "catch_weight_ind", nullable = true) // This field can be NULL
    @SerializedName("catchWeightInd")
    private String catchWeightInd;

    @Column(name = "class_id", nullable = true) // This field can be NULL
    @SerializedName("classId")
    private Integer classId;

    @Column(name = "colour_dsc", nullable = true) // This field can be NULL
    @SerializedName("colourDsc")
    private String colourDsc;

    @Column(name = "colour_group_id", nullable = true) // This field can be NULL
    @SerializedName("colourGroupId")
    private String colourGroupId;

    @Column(name = "colour_id", nullable = true) // This field can be NULL
    @SerializedName("colourId")
    private String colourId;

    @Column(name = "colour_range_id", nullable = true) // This field can be NULL
    @SerializedName("colourRangeId")
    private Integer colourRangeId;

    @Column(name = "company_id", nullable = true) // This field can be NULL
    @SerializedName("companyId")
    private Integer companyId;

    @Column(name = "count_on_us_id", nullable = true) // This field can be NULL
    @SerializedName("countOnUsId")
    private String countOnUsId;

    @Column(name = "create_dte", nullable = true) // This field can be NULL
    @SerializedName("createDte")
    private LocalDateTime createDte;

    @Column(name = "dept_id", nullable = true) // This field can be NULL
    @SerializedName("deptId")
    private Integer deptId;

    @Column(name = "discipline", nullable = true) // This field can be NULL
    @SerializedName("discipline")
    private String discipline;

    @Column(name = "domain_id", nullable = true) // This field can be NULL
    @SerializedName("domainId")
    private Integer domainId;

    @Column(name = "flavour_dsc", nullable = true) // This field can be NULL
    @SerializedName("flavourDsc")
    private String flavourDsc;

    @Column(name = "flavour_group_id", nullable = true) // This field can be NULL
    @SerializedName("flavourGroupId")
    private String flavourGroupId;

    @Column(name = "flavour_id", nullable = true) // This field can be NULL
    @SerializedName("flavourId")
    private String flavourId;

    @Column(name = "flavour_range_id", nullable = true) // This field can be NULL
    @SerializedName("flavourRangeId")
    private Integer flavourRangeId;

    @Column(name = "forecast_ind", nullable = true) // This field can be NULL
    @SerializedName("forecastInd")
    private String forecastInd;

    @Column(name = "free_range_id", nullable = true) // This field can be NULL
    @SerializedName("freeRangeId")
    private String freeRangeId;

    @Column(name = "from_temp", nullable = true) // This field can be NULL
    @SerializedName("fromTemp")
    private Integer fromTemp;

    @Column(name = "group_id", nullable = true) // This field can be NULL
    @SerializedName("groupId")
    private Integer groupId;

    @Column(name = "high_max_temp", nullable = true) // This field can be NULL
    @SerializedName("highMaxTemp")
    private Integer highMaxTemp;

    @Column(name = "high_min_temp", nullable = true) // This field can be NULL
    @SerializedName("highMinTemp")
    private Integer highMinTemp;

    @Column(name = "item_grandparent", nullable = true) // This field can be NULL
    @SerializedName("itemGrandparent")
    private String itemGrandparent;

    @Column(name = "item_parent", nullable = true) // This field can be NULL
    @SerializedName("itemParent")
    private String itemParent;

    @Column(name = "kidz_id", nullable = true) // This field can be NULL
    @SerializedName("kidzId")
    private String kidzId;

    @Column(name = "orderable_ind", nullable = true) // This field can be NULL
    @SerializedName("orderableInd")
    private String orderableInd;

    @Column(name = "pack_ind", nullable = true) // This field can be NULL
    @SerializedName("packInd")
    private String packInd;

    @Column(name = "pack_member", nullable = true) // This field can be NULL
    @SerializedName("packMember")
    private String packMember;

    @Column(name = "pack_qty", nullable = true) // This field can be NULL
    @SerializedName("packQty")
    private BigDecimal packQty;

    @Column(name = "phase_id", nullable = true) // This field can be NULL
    @SerializedName("phaseId")
    private Integer phaseId;

    @Column(name = "price_mark_ind", nullable = true) // This field can be NULL
    @SerializedName("priceMarkInd")
    private String priceMarkInd;

    @Column(name = "primary_ref_item_ind", nullable = true) // This field can be NULL
    @SerializedName("primaryRefItemInd")
    private String primaryRefItemInd;

    @Column(name = "primary_size_dsc", nullable = true) // This field can be NULL
    @SerializedName("primarySizeDsc")
    private String primarySizeDsc;

    @Column(name = "primary_size_group_id", nullable = true) // This field can be NULL
    @SerializedName("primarySizeGroupId")
    private String primarySizeGroupId;

    @Column(name = "primary_size_id", nullable = true) // This field can be NULL
    @SerializedName("primarySizeId")
    private String primarySizeId;

    @Column(name = "primary_size_range_id", nullable = true) // This field can be NULL
    @SerializedName("primarySizeRangeId")
    private Integer primarySizeRangeId;

    @Column(name = "product_group_scaling", nullable = true) // This field can be NULL
    @SerializedName("productGroupScaling")
    private String productGroupScaling;

    @Column(name = "product_id", nullable = true) // This field can be NULL
    @SerializedName("productId")
    private String productId;

    @Column(name = "reference_item_ind", nullable = true) // This field can be NULL
    @SerializedName("referenceItemInd")
    private String referenceItemInd;

    @Column(name = "scent_dsc", nullable = true) // This field can be NULL
    @SerializedName("scentDsc")
    private String scentDsc;

    @Column(name = "scent_group_id", nullable = true) // This field can be NULL
    @SerializedName("scentGroupId")
    private String scentGroupId;

    @Column(name = "scent_id", nullable = true) // This field can be NULL
    @SerializedName("scentId")
    private String scentId;

    @Column(name = "scent_range_id", nullable = true) // This field can be NULL
    @SerializedName("scentRangeId")
    private Integer scentRangeId;

    @Column(name = "season_id", nullable = true) // This field can be NULL
    @SerializedName("seasonId")
    private Integer seasonId;

    @Column(name = "secondary_size_dsc", nullable = true) // This field can be NULL
    @SerializedName("secondarySizeDsc")
    private String secondarySizeDsc;

    @Column(name = "secondary_size_group_id", nullable = true) // This field can be NULL
    @SerializedName("secondarySizeGroupId")
    private String secondarySizeGroupId;

    @Column(name = "secondary_size_id", nullable = true) // This field can be NULL
    @SerializedName("secondarySizeId")
    private String secondarySizeId;

    @Column(name = "secondary_size_range_id", nullable = true) // This field can be NULL
    @SerializedName("secondarySizeRangeId")
    private Integer secondarySizeRangeId;

    @Column(name = "sellable_ind", nullable = true) // This field can be NULL
    @SerializedName("sellableInd")
    private String sellableInd;

    @Column(name = "short_dsc", nullable = true) // This field can be NULL
    @SerializedName("shortDsc")
    private String shortDsc;

    @Column(name = "simple_pack_ind", nullable = true) // This field can be NULL
    @SerializedName("simplePackInd")
    private String simplePackInd;

    @Column(name = "size_profile_ind", nullable = true) // This field can be NULL
    @SerializedName("sizeProfileInd")
    private String sizeProfileInd;

    @Column(name = "standard_uom", nullable = true) // This field can be NULL
    @SerializedName("standardUom")
    private String standardUom;

    @Column(name = "status", nullable = true) // This field can be NULL
    @SerializedName("status")
    private String status;

    @Column(name = "sub_group_id", nullable = true) // This field can be NULL
    @SerializedName("subGroupId")
    private Integer subGroupId;

    @Column(name = "subclass_id", nullable = true) // This field can be NULL
    @SerializedName("subclassId")
    private Integer subclassId;

    @Column(name = "supplier_no", nullable = true) // This field can be NULL
    @SerializedName("supplierNo")
    private Integer supplierNo;

    @Column(name = "to_temp", nullable = true) // This field can be NULL
    @SerializedName("toTemp")
    private Integer toTemp;

    @Column(name = "tran_ind", nullable = true) // This field can be NULL
    @SerializedName("tranInd")
    private String tranInd;

    @Column(name = "tran_level", nullable = true) // This field can be NULL
    @SerializedName("tranLevel")
    private Integer tranLevel;

    @Column(name = "ww_colour", nullable = true) // This field can be NULL
    @SerializedName("wwColour")
    private String wwColour;

    @Column(name = "ww_size", nullable = true) // This field can be NULL
    @SerializedName("wwSize")
    private String wwSize;

    @Column(name = "ww_static_mass", nullable = true) // This field can be NULL
    @SerializedName("wwStaticMass")
    private BigDecimal wwStaticMass;

    @Column(name = "ww_style", nullable = true) // This field can be NULL
    @SerializedName("wwStyle")
    private String wwStyle;

    @Column(name = "ww_style_colour", nullable = true) // This field can be NULL
    @SerializedName("wwStyleColour")
    private String wwStyleColour;

    @Column(name = "variable_weight_ind", nullable = true) // This field can be NULL
    @SerializedName("variableWeightInd")
    private Character variableWeightInd;

    @Column(name = "loose_prod_ind", nullable = true) // This field can be NULL
    @SerializedName("looseProdInd")
    private Character looseProdInd;

    @Column(name = "item_scale_ind", nullable = true) // This field can be NULL
    @SerializedName("itemScaleInd")
    private Character itemScaleInd;

    @Column(name = "legacy_sku_no", nullable = true) // This field can be NULL
    @SerializedName("legacySkuNo")
    private String legacySkuNo;

    @Column(name = "legacy_random_mass_ind", nullable = true) // This field can be NULL
    @SerializedName("legacyRandomMassInd")
    private Character legacyRandomMassInd;

    @Column(name = "legacy_vat_ind", nullable = true) // This field can be NULL
    @SerializedName("legacyVatInd")
    private Character legacyVatInd;

    @Column(name = "action_ind", nullable = true) // This field can be NULL
    @SerializedName("actionInd")
    private Character actionInd;

    @Column(name = "extract_seq_no", nullable = true) // This field can be NULL
    @SerializedName("extractSeqNo")
    private Long extractSeqNo;

    @Column(name = "vat_cde", nullable = true) // This field can be NULL
    @SerializedName("vatCde")
    private String vatCde;

    @Column(name = "vat_rate", nullable = true) // This field can be NULL
    @SerializedName("vatRate")
    private BigDecimal vatRate;

    @Column(name = "source_system", nullable = true) // This field can be NULL
    @SerializedName("sourceSystem")
    private String sourceSystem;

    @Column(name = "vpn_no", nullable = true) // This field can be NULL
    @SerializedName("vpnNo")
    private String vpnNo;

    @Column(name = "ext_ref_no", nullable = true) // This field can be NULL
    @SerializedName("extRefNo")
    private String extRefNo;

    @Column(name = "item_long_desc", nullable = true) // This field can be NULL
    @SerializedName("itemLongDesc")
    private String itemLongDesc;

    @Column(name = "segregation_ind", nullable = true) // This field can be NULL
    @SerializedName("segregationInd")
    private String segregationInd;

    @Column(name = "prod_class", nullable = true) // This field can be NULL
    @SerializedName("prodClass")
    private String prodClass;

    @Column(name = "last_update_dte", nullable = true) // This field can be NULL
    @SerializedName("lastUpdateDte")
    private LocalDateTime lastUpdateDte;

    // Getters and Setters

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Integer getItemLevel() {
        return itemLevel;
    }

    public void setItemLevel(Integer itemLevel) {
        this.itemLevel = itemLevel;
    }

    public String getItemNumberType() {
        return itemNumberType;
    }

    public void setItemNumberType(String itemNumberType) {
        this.itemNumberType = itemNumberType;
    }

    public Integer getPrefix() {
        return prefix;
    }

    public void setPrefix(Integer prefix) {
        this.prefix = prefix;
    }

    public String getAllocatorSystem() {
        return allocatorSystem;
    }

    public void setAllocatorSystem(String allocatorSystem) {
        this.allocatorSystem = allocatorSystem;
    }

    public Integer getBusinessUnitId() {
        return businessUnitId;
    }

    public void setBusinessUnitId(Integer businessUnitId) {
        this.businessUnitId = businessUnitId;
    }

    public String getCatchWeightInd() {
        return catchWeightInd;
    }

    public void setCatchWeightInd(String catchWeightInd) {
        this.catchWeightInd = catchWeightInd;
    }

    public Integer getClassId() {
        return classId;
    }

    public void setClassId(Integer classId) {
        this.classId = classId;
    }

    public String getColourDsc() {
        return colourDsc;
    }

    public void setColourDsc(String colourDsc) {
        this.colourDsc = colourDsc;
    }

    public String getColourGroupId() {
        return colourGroupId;
    }

    public void setColourGroupId(String colourGroupId) {
        this.colourGroupId = colourGroupId;
    }

    public String getColourId() {
        return colourId;
    }

    public void setColourId(String colourId) {
        this.colourId = colourId;
    }

    public Integer getColourRangeId() {
        return colourRangeId;
    }

    public void setColourRangeId(Integer colourRangeId) {
        this.colourRangeId = colourRangeId;
    }

    public Integer getCompanyId() {
        return companyId;
    }

    public void setCompanyId(Integer companyId) {
        this.companyId = companyId;
    }

    public String getCountOnUsId() {
        return countOnUsId;
    }

    public void setCountOnUsId(String countOnUsId) {
        this.countOnUsId = countOnUsId;
    }

    public LocalDateTime getCreateDte() {
        return createDte;
    }

    public void setCreateDte(LocalDateTime createDte) {
        this.createDte = createDte;
    }

    public Integer getDeptId() {
        return deptId;
    }

    public void setDeptId(Integer deptId) {
        this.deptId = deptId;
    }

    public String getDiscipline() {
        return discipline;
    }

    public void setDiscipline(String discipline) {
        this.discipline = discipline;
    }

    public Integer getDomainId() {
        return domainId;
    }

    public void setDomainId(Integer domainId) {
        this.domainId = domainId;
    }

    public String getFlavourDsc() {
        return flavourDsc;
    }

    public void setFlavourDsc(String flavourDsc) {
        this.flavourDsc = flavourDsc;
    }

    public String getFlavourGroupId() {
        return flavourGroupId;
    }

    public void setFlavourGroupId(String flavourGroupId) {
        this.flavourGroupId = flavourGroupId;
    }

    public String getFlavourId() {
        return flavourId;
    }

    public void setFlavourId(String flavourId) {
        this.flavourId = flavourId;
    }

    public Integer getFlavourRangeId() {
        return flavourRangeId;
    }

    public void setFlavourRangeId(Integer flavourRangeId) {
        this.flavourRangeId = flavourRangeId;
    }

    public String getForecastInd() {
        return forecastInd;
    }

    public void setForecastInd(String forecastInd) {
        this.forecastInd = forecastInd;
    }

    public String getFreeRangeId() {
        return freeRangeId;
    }

    public void setFreeRangeId(String freeRangeId) {
        this.freeRangeId = freeRangeId;
    }

    public Integer getFromTemp() {
        return fromTemp;
    }

    public void setFromTemp(Integer fromTemp) {
        this.fromTemp = fromTemp;
    }

    public Integer getGroupId() {
        return groupId;
    }

    public void setGroupId(Integer groupId) {
        this.groupId = groupId;
    }

    public Integer getHighMaxTemp() {
        return highMaxTemp;
    }

    public void setHighMaxTemp(Integer highMaxTemp) {
        this.highMaxTemp = highMaxTemp;
    }

    public Integer getHighMinTemp() {
        return highMinTemp;
    }

    public void setHighMinTemp(Integer highMinTemp) {
        this.highMinTemp = highMinTemp;
    }

    public String getItemGrandparent() {
        return itemGrandparent;
    }

    public void setItemGrandparent(String itemGrandparent) {
        this.itemGrandparent = itemGrandparent;
    }

    public String getItemParent() {
        return itemParent;
    }

    public void setItemParent(String itemParent) {
        this.itemParent = itemParent;
    }

    public String getKidzId() {
        return kidzId;
    }

    public void setKidzId(String kidzId) {
        this.kidzId = kidzId;
    }

    public String getOrderableInd() {
        return orderableInd;
    }

    public void setOrderableInd(String orderableInd) {
        this.orderableInd = orderableInd;
    }

    public String getPackInd() {
        return packInd;
    }

    public void setPackInd(String packInd) {
        this.packInd = packInd;
    }

    public String getPackMember() {
        return packMember;
    }

    public void setPackMember(String packMember) {
        this.packMember = packMember;
    }

    public BigDecimal getPackQty() {
        return packQty;
    }

    public void setPackQty(BigDecimal packQty) {
        this.packQty = packQty;
    }

    public Integer getPhaseId() {
        return phaseId;
    }

    public void setPhaseId(Integer phaseId) {
        this.phaseId = phaseId;
    }

    public String getPriceMarkInd() {
        return priceMarkInd;
    }

    public void setPriceMarkInd(String priceMarkInd) {
        this.priceMarkInd = priceMarkInd;
    }

    public String getPrimaryRefItemInd() {
        return primaryRefItemInd;
    }

    public void setPrimaryRefItemInd(String primaryRefItemInd) {
        this.primaryRefItemInd = primaryRefItemInd;
    }

    public String getPrimarySizeDsc() {
        return primarySizeDsc;
    }

    public void setPrimarySizeDsc(String primarySizeDsc) {
        this.primarySizeDsc = primarySizeDsc;
    }

    public String getPrimarySizeGroupId() {
        return primarySizeGroupId;
    }

    public void setPrimarySizeGroupId(String primarySizeGroupId) {
        this.primarySizeGroupId = primarySizeGroupId;
    }

    public String getPrimarySizeId() {
        return primarySizeId;
    }

    public void setPrimarySizeId(String primarySizeId) {
        this.primarySizeId = primarySizeId;
    }

    public Integer getPrimarySizeRangeId() {
        return primarySizeRangeId;
    }

    public void setPrimarySizeRangeId(Integer primarySizeRangeId) {
        this.primarySizeRangeId = primarySizeRangeId;
    }

    public String getProductGroupScaling() {
        return productGroupScaling;
    }

    public void setProductGroupScaling(String productGroupScaling) {
        this.productGroupScaling = productGroupScaling;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getReferenceItemInd() {
        return referenceItemInd;
    }

    public void setReferenceItemInd(String referenceItemInd) {
        this.referenceItemInd = referenceItemInd;
    }

    public String getScentDsc() {
        return scentDsc;
    }

    public void setScentDsc(String scentDsc) {
        this.scentDsc = scentDsc;
    }

    public String getScentGroupId() {
        return scentGroupId;
    }

    public void setScentGroupId(String scentGroupId) {
        this.scentGroupId = scentGroupId;
    }

    public String getScentId() {
        return scentId;
    }

    public void setScentId(String scentId) {
        this.scentId = scentId;
    }

    public Integer getScentRangeId() {
        return scentRangeId;
    }

    public void setScentRangeId(Integer scentRangeId) {
        this.scentRangeId = scentRangeId;
    }

    public Integer getSeasonId() {
        return seasonId;
    }

    public void setSeasonId(Integer seasonId) {
        this.seasonId = seasonId;
    }

    public String getSecondarySizeDsc() {
        return secondarySizeDsc;
    }

    public void setSecondarySizeDsc(String secondarySizeDsc) {
        this.secondarySizeDsc = secondarySizeDsc;
    }

    public String getSecondarySizeGroupId() {
        return secondarySizeGroupId;
    }

    public void setSecondarySizeGroupId(String secondarySizeGroupId) {
        this.secondarySizeGroupId = secondarySizeGroupId;
    }

    public String getSecondarySizeId() {
        return secondarySizeId;
    }

    public void setSecondarySizeId(String secondarySizeId) {
        this.secondarySizeId = secondarySizeId;
    }

    public Integer getSecondarySizeRangeId() {
        return secondarySizeRangeId;
    }

    public void setSecondarySizeRangeId(Integer secondarySizeRangeId) {
        this.secondarySizeRangeId = secondarySizeRangeId;
    }

    public String getSellableInd() {
        return sellableInd;
    }

    public void setSellableInd(String sellableInd) {
        this.sellableInd = sellableInd;
    }

    public String getShortDsc() {
        return shortDsc;
    }

    public void setShortDsc(String shortDsc) {
        this.shortDsc = shortDsc;
    }

    public String getSimplePackInd() {
        return simplePackInd;
    }

    public void setSimplePackInd(String simplePackInd) {
        this.simplePackInd = simplePackInd;
    }

    public String getSizeProfileInd() {
        return sizeProfileInd;
    }

    public void setSizeProfileInd(String sizeProfileInd) {
        this.sizeProfileInd = sizeProfileInd;
    }

    public String getStandardUom() {
        return standardUom;
    }

    public void setStandardUom(String standardUom) {
        this.standardUom = standardUom;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getSubGroupId() {
        return subGroupId;
    }

    public void setSubGroupId(Integer subGroupId) {
        this.subGroupId = subGroupId;
    }

    public Integer getSubclassId() {
        return subclassId;
    }

    public void setSubclassId(Integer subclassId) {
        this.subclassId = subclassId;
    }

    public Integer getSupplierNo() {
        return supplierNo;
    }

    public void setSupplierNo(Integer supplierNo) {
        this.supplierNo = supplierNo;
    }

    public Integer getToTemp() {
        return toTemp;
    }

    public void setToTemp(Integer toTemp) {
        this.toTemp = toTemp;
    }

    public String getTranInd() {
        return tranInd;
    }

    public void setTranInd(String tranInd) {
        this.tranInd = tranInd;
    }

    public Integer getTranLevel() {
        return tranLevel;
    }

    public void setTranLevel(Integer tranLevel) {
        this.tranLevel = tranLevel;
    }

    public String getWwColour() {
        return wwColour;
    }

    public void setWwColour(String wwColour) {
        this.wwColour = wwColour;
    }

    public String getWwSize() {
        return wwSize;
    }

    public void setWwSize(String wwSize) {
        this.wwSize = wwSize;
    }

    public BigDecimal getWwStaticMass() {
        return wwStaticMass;
    }

    public void setWwStaticMass(BigDecimal wwStaticMass) {
        this.wwStaticMass = wwStaticMass;
    }

    public String getWwStyle() {
        return wwStyle;
    }

    public void setWwStyle(String wwStyle) {
        this.wwStyle = wwStyle;
    }

    public String getWwStyleColour() {
        return wwStyleColour;
    }

    public void setWwStyleColour(String wwStyleColour) {
        this.wwStyleColour = wwStyleColour;
    }

    public Character getVariableWeightInd() {
        return variableWeightInd;
    }

    public void setVariableWeightInd(Character variableWeightInd) {
        this.variableWeightInd = variableWeightInd;
    }

    public Character getLooseProdInd() {
        return looseProdInd;
    }

    public void setLooseProdInd(Character looseProdInd) {
        this.looseProdInd = looseProdInd;
    }

    public Character getItemScaleInd() {
        return itemScaleInd;
    }

    public void setItemScaleInd(Character itemScaleInd) {
        this.itemScaleInd = itemScaleInd;
    }

    public String getLegacySkuNo() {
        return legacySkuNo;
    }

    public void setLegacySkuNo(String legacySkuNo) {
        this.legacySkuNo = legacySkuNo;
    }

    public Character getLegacyRandomMassInd() {
        return legacyRandomMassInd;
    }

    public void setLegacyRandomMassInd(Character legacyRandomMassInd) {
        this.legacyRandomMassInd = legacyRandomMassInd;
    }

    public Character getLegacyVatInd() {
        return legacyVatInd;
    }

    public void setLegacyVatInd(Character legacyVatInd) {
        this.legacyVatInd = legacyVatInd;
    }

    public Character getActionInd() {
        return actionInd;
    }

    public void setActionInd(Character actionInd) {
        this.actionInd = actionInd;
    }

    public Long getExtractSeqNo() {
        return extractSeqNo;
    }

    public void setExtractSeqNo(Long extractSeqNo) {
        this.extractSeqNo = extractSeqNo;
    }

    public String getVatCde() {
        return vatCde;
    }

    public void setVatCde(String vatCde) {
        this.vatCde = vatCde;
    }

    public BigDecimal getVatRate() {
        return vatRate;
    }

    public void setVatRate(BigDecimal vatRate) {
        this.vatRate = vatRate;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getVpnNo() {
        return vpnNo;
    }

    public void setVpnNo(String vpnNo) {
        this.vpnNo = vpnNo;
    }

    public String getExtRefNo() {
        return extRefNo;
    }

    public void setExtRefNo(String extRefNo) {
        this.extRefNo = extRefNo;
    }

    public String getItemLongDesc() {
        return itemLongDesc;
    }

    public void setItemLongDesc(String itemLongDesc) {
        this.itemLongDesc = itemLongDesc;
    }

    public String getSegregationInd() {
        return segregationInd;
    }

    public void setSegregationInd(String segregationInd) {
        this.segregationInd = segregationInd;
    }

    public String getProdClass() {
        return prodClass;
    }

    public void setProdClass(String prodClass) {
        this.prodClass = prodClass;
    }

    public LocalDateTime getLastUpdateDte() {
        return lastUpdateDte;
    }

    public void setLastUpdateDte(LocalDateTime lastUpdateDte) {
        this.lastUpdateDte = lastUpdateDte;
    }

}

