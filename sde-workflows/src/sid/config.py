"""
SID Transformation Configuration

Contains FK relationships and processing order derived from MDF schema analysis.
Source: AIRExposure.mdf (s3://rks-sd-virusscan/extraction-test/)
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class TableConfig:
    """Configuration for a single table's SID columns."""
    pk_column: str
    fk_columns: Dict[str, str]  # {fk_column_name: parent_table_name}
    self_reference_fk: Optional[str] = None  # Column that references same table's PK


# Processing levels based on FK dependencies (parent tables first)
PROCESSING_LEVELS: Dict[int, List[str]] = {
    0: [  # Root tables — no FK dependencies
        "tCompany",
        "tPortfolioFilter",
        "tAppliesToArea",
        "tReinsuranceEPCurveSet",
        "tSIDControl",
    ],
    1: [  # Depend only on Level 0
        "tExposureSet",
        "tReinsuranceProgram",
        "tPortfolio",
        "tReinsuranceEPCurve",
        "tCompanyCatBondImportHistory",
        "tCompanyUDCClassification_Xref",
    ],
    2: [  # Depend on Level 1
        "tContract",
        "tReinsuranceTreaty",
        "tCompanyLossAssociation",
        "tReinsuranceProgramCoverage",
        "tReinsuranceProgramEPAdjustment",
        "tReinsuranceProgramEPAdjustmentPoints",
        "tReinsuranceProgramJapanCFLRatio",
        "tReinsuranceProgramSourceInure",
        "tReinsuranceProgramTargetXRef",
        "tCatBondAttributeValue",
        "tReinsAppliesToExp",
        "tReinsuranceExposureAdjustmentFactor",
        "tAggregateExposure",
        "tReinsuranceEPCurveAdjustmentPoint",
    ],
    3: [  # Depend on Level 2
        "tLocation",  # Has self-reference: ParentLocationSID
        "tLayer",
        "tStepFunction",
        "tAppliesToEventFilter",
        "tReinsuranceTreatyReinstatement",
        "tReinsuranceTreatySavedResults",
        "tReinsuranceTreatyTerrorismOption",
        "tReinsuranceTreatyUDCClassification_Xref",
        "tPortfolioReinsuranceTreaty",
        "tCLFCatalog",
        "tCompanyLossMarketShare",
        "tCompanyCLASizes",
    ],
    4: [  # Depend on Level 3
        "tLayerCondition",  # Has self-reference: ParentLayerConditionSID
        "tLocTerm",
        "tLocFeature",
        "tLocOffshore",
        "tLocParsedAddress",
        "tLocWC",
        "tEngineLocPhysicalProperty",
        "tStepFunctionDetail",
        "tAppliesToEventFilterGeoXRef",
        "tAppliesToEventFilterLatLong",
        "tAppliesToEventFilterRule",
        "tProgramBusinessCategoryFactor",
    ],
    5: [  # Junction/child tables — depend on multiple parents
        "tLayerConditionLocationXref",
        "tLocStepFunctionXref",
        "tAppliesToAreaGeoXRef",
    ],
}


# Table configurations with PK and FK relationships
# Derived from MDF schema analysis
SID_CONFIG: Dict[str, TableConfig] = {
    # Level 0 - Root tables
    "tCompany": TableConfig(
        pk_column="CompanySID",
        fk_columns={
            "BusinessUnitSID": "EXTERNAL",  # External reference, not transformed
            "DefaultExposureSetSID": "tExposureSet",  # Circular - handle specially
        },
    ),
    "tPortfolioFilter": TableConfig(
        pk_column="PortfolioFilterSID",
        fk_columns={},
    ),
    "tAppliesToArea": TableConfig(
        pk_column="AppliesToAreaSID",
        fk_columns={},
    ),
    "tReinsuranceEPCurveSet": TableConfig(
        pk_column="ReinsuranceEPCurveSetSID",
        fk_columns={
            "CompanySID": "tCompany",
            "CurrencyExchangeRateSetSID": "EXTERNAL",
            "ReinsuranceEventSetSID": "EXTERNAL",
            "ULFCurrencyExchangeRateSetSID": "EXTERNAL",
        },
    ),
    "tSIDControl": TableConfig(
        pk_column="TableSID",
        fk_columns={},
    ),
    
    # Level 1 - Depend on Level 0
    "tExposureSet": TableConfig(
        pk_column="ExposureSetSID",
        fk_columns={
            "CompanySID": "tCompany",
        },
    ),
    "tReinsuranceProgram": TableConfig(
        pk_column="ReinsuranceProgramSID",
        fk_columns={
            "CompanySID": "tCompany",
            "BusinessUnitSID": "EXTERNAL",
            "CurrencyExchangeRateSetSID": "EXTERNAL",
            "ProjectSID": "EXTERNAL",
        },
    ),
    "tPortfolio": TableConfig(
        pk_column="PortfolioSID",
        fk_columns={
            "PortfolioFilterSID": "tPortfolioFilter",
            "BusinessUnitSID": "EXTERNAL",
        },
    ),
    "tReinsuranceEPCurve": TableConfig(
        pk_column="ReinsuranceEPCurveSID",
        fk_columns={
            "ReinsuranceEPCurveSetSID": "tReinsuranceEPCurveSet",
            "AppliesToAreaSID": "tAppliesToArea",
        },
    ),
    "tCompanyCatBondImportHistory": TableConfig(
        pk_column="CompanyCatBondImportHistorySID",
        fk_columns={
            "CompanySID": "tCompany",
        },
    ),
    "tCompanyUDCClassification_Xref": TableConfig(
        pk_column="CompanySID",  # Composite PK
        fk_columns={
            "CompanySID": "tCompany",
            "UDCClassificationSID": "EXTERNAL",
        },
    ),
    
    # Level 2 - Depend on Level 1
    "tContract": TableConfig(
        pk_column="ContractSID",
        fk_columns={
            "ExposureSetSID": "tExposureSet",
        },
    ),
    "tReinsuranceTreaty": TableConfig(
        pk_column="ReinsuranceTreatySID",
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
            "AppliesToAreaSID": "tAppliesToArea",
        },
    ),
    "tCompanyLossAssociation": TableConfig(
        pk_column="CompanyLossAssociationSID",
        fk_columns={
            "CompanySID": "tCompany",
            "CompanyLossSetSID": "EXTERNAL",
            "SourceBusinessUnitSID": "EXTERNAL",
            "SqlInstanceSID": "EXTERNAL",
        },
        self_reference_fk="ParentCompanyLossAssociationSID",
    ),
    "tReinsuranceProgramCoverage": TableConfig(
        pk_column="ReinsuranceProgramSID",
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
        },
    ),
    "tReinsuranceProgramEPAdjustment": TableConfig(
        pk_column="ReinsuranceProgramSID",
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
        },
    ),
    "tReinsuranceProgramEPAdjustmentPoints": TableConfig(
        pk_column="ReinsuranceProgramSID",
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
        },
    ),
    "tReinsuranceProgramJapanCFLRatio": TableConfig(
        pk_column="ReinsuranceProgramSID",  # Composite PK
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
            "GeographySID": "EXTERNAL",
            "ReinsuranceEventSetSID": "EXTERNAL",
        },
    ),
    "tReinsuranceProgramSourceInure": TableConfig(
        pk_column="ReinsuranceProgramSubjectSID",  # Composite PK
        fk_columns={
            "ReinsuranceProgramSourceInureSID": "tReinsuranceProgram",
            "ReinsuranceProgramSubjectSID": "tReinsuranceProgram",
        },
    ),
    "tReinsuranceProgramTargetXRef": TableConfig(
        pk_column="ReinsuranceProgramSID",
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
            "ExposureSetSID": "tExposureSet",
            "CompanyLossAssociationSID": "tCompanyLossAssociation",
            "YearSID": "EXTERNAL",
        },
    ),
    "tCatBondAttributeValue": TableConfig(
        pk_column="ReinsuranceProgramSID",  # Composite PK
        fk_columns={
            "ReinsuranceProgramSID": "tReinsuranceProgram",
            "CatBondAttributeSID": "EXTERNAL",
        },
    ),
    "tReinsAppliesToExp": TableConfig(
        pk_column="ReinsAppliesToExpSID",
        fk_columns={
            "ExposureSetSID": "tExposureSet",
            "ReinsTargetSID": "EXTERNAL",
        },
    ),
    "tReinsuranceExposureAdjustmentFactor": TableConfig(
        pk_column="ReinsuranceExposureAdjustmentFactorSID",
        fk_columns={
            "ExposureSetSID": "tExposureSet",
        },
    ),
    "tAggregateExposure": TableConfig(
        pk_column="AggregateExposureSID",
        fk_columns={
            "ExposureSetSID": "tExposureSet",
            "GeographySID": "EXTERNAL",
        },
    ),
    "tReinsuranceEPCurveAdjustmentPoint": TableConfig(
        pk_column="ReinsuranceEPCurveSID",
        fk_columns={
            "ReinsuranceEPCurveSID": "tReinsuranceEPCurve",
        },
    ),
    
    # Level 3 - Depend on Level 2
    "tLocation": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "ContractSID": "tContract",
            "ExposureSetSID": "tExposureSet",
            "GeographySID": "EXTERNAL",
        },
        self_reference_fk="ParentLocationSID",
    ),
    "tLayer": TableConfig(
        pk_column="LayerSID",
        fk_columns={
            "ContractSID": "tContract",
        },
    ),
    "tStepFunction": TableConfig(
        pk_column="StepFunctionSID",
        fk_columns={
            "ContractSID": "tContract",
        },
    ),
    "tAppliesToEventFilter": TableConfig(
        pk_column="AppliesToEventFilterSID",
        fk_columns={
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
        },
    ),
    "tReinsuranceTreatyReinstatement": TableConfig(
        pk_column="ReinsuranceTreatyReinstatementSID",
        fk_columns={
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
        },
    ),
    "tReinsuranceTreatySavedResults": TableConfig(
        pk_column="ReinsuranceTreatySavedResultsID",
        fk_columns={
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
            "CurrencyExchangeRateSetSID": "EXTERNAL",
            "EventBasedAdjFactorSetSID": "EXTERNAL",
        },
    ),
    "tReinsuranceTreatyTerrorismOption": TableConfig(
        pk_column="ReinsuranceTreatySID",
        fk_columns={
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
        },
    ),
    "tReinsuranceTreatyUDCClassification_Xref": TableConfig(
        pk_column="ReinsuranceTreatySID",  # Composite PK
        fk_columns={
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
            "UDCClassificationSID": "EXTERNAL",
        },
    ),
    "tPortfolioReinsuranceTreaty": TableConfig(
        pk_column="PortfolioSID",  # Composite PK
        fk_columns={
            "PortfolioSID": "tPortfolio",
            "ReinsuranceTreatySID": "tReinsuranceTreaty",
        },
    ),
    "tCLFCatalog": TableConfig(
        pk_column="CLFCatalogSID",
        fk_columns={
            "CompanyLossAssociationSID": "tCompanyLossAssociation",
        },
    ),
    "tCompanyLossMarketShare": TableConfig(
        pk_column="CompanyLossMarketShareSID",
        fk_columns={
            "CompanyLossAssociationSID": "tCompanyLossAssociation",
            "CountrySID": "EXTERNAL",
            "GeographySID": "EXTERNAL",
        },
    ),
    "tCompanyCLASizes": TableConfig(
        pk_column="CompanySID",  # Composite PK
        fk_columns={
            "CompanySID": "tCompany",
            "CompanyLossSetSID": "EXTERNAL",
        },
    ),
    
    # Level 4 - Depend on Level 3
    "tLayerCondition": TableConfig(
        pk_column="LayerConditionSID",
        fk_columns={
            "ContractSID": "tContract",
            "LayerSID": "tLayer",
        },
        self_reference_fk="ParentLayerConditionSID",
    ),
    "tLocTerm": TableConfig(
        pk_column="LocTermSID",
        fk_columns={
            "ContractSID": "tContract",
            "LocationSID": "tLocation",
        },
    ),
    "tLocFeature": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "LocationSID": "tLocation",
        },
    ),
    "tLocOffshore": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "LocationSID": "tLocation",
        },
    ),
    "tLocParsedAddress": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "LocationSID": "tLocation",
        },
    ),
    "tLocWC": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "LocationSID": "tLocation",
        },
    ),
    "tEngineLocPhysicalProperty": TableConfig(
        pk_column="LocationSID",
        fk_columns={
            "LocationSID": "tLocation",
        },
    ),
    "tStepFunctionDetail": TableConfig(
        pk_column="StepFunctionSID",
        fk_columns={
            "StepFunctionSID": "tStepFunction",
        },
    ),
    "tAppliesToEventFilterGeoXRef": TableConfig(
        pk_column="AppliesToEventFilterSID",  # Composite PK
        fk_columns={
            "AppliesToEventFilterSID": "tAppliesToEventFilter",
            "GeographySID": "EXTERNAL",
        },
    ),
    "tAppliesToEventFilterLatLong": TableConfig(
        pk_column="AppliesToEventFilterSID",
        fk_columns={
            "AppliesToEventFilterSID": "tAppliesToEventFilter",
        },
    ),
    "tAppliesToEventFilterRule": TableConfig(
        pk_column="AppliesToEventFilterRuleSID",
        fk_columns={
            "AppliesToEventFilterSID": "tAppliesToEventFilter",
        },
    ),
    "tProgramBusinessCategoryFactor": TableConfig(
        pk_column="ContractSID",
        fk_columns={
            "ContractSID": "tContract",
        },
    ),
    
    # Level 5 - Junction tables
    "tLayerConditionLocationXref": TableConfig(
        pk_column="LayerConditionSID",  # Composite PK
        fk_columns={
            "LayerConditionSID": "tLayerCondition",
            "LocationSID": "tLocation",
        },
    ),
    "tLocStepFunctionXref": TableConfig(
        pk_column="LocationSID",  # Composite PK
        fk_columns={
            "LocationSID": "tLocation",
            "StepFunctionSID": "tStepFunction",
        },
    ),
    "tAppliesToAreaGeoXRef": TableConfig(
        pk_column="AppliesToAreaSID",  # Composite PK
        fk_columns={
            "AppliesToAreaSID": "tAppliesToArea",
            "GeographySID": "EXTERNAL",
        },
    ),
}


def get_processing_order() -> List[Tuple[int, str]]:
    """
    Get tables in dependency order for SID transformation.
    
    Returns:
        List of (level, table_name) tuples in processing order.
    """
    result = []
    for level in sorted(PROCESSING_LEVELS.keys()):
        for table in PROCESSING_LEVELS[level]:
            result.append((level, table))
    return result


def get_fk_relationships(table_name: str) -> Dict[str, str]:
    """
    Get FK relationships for a table.
    
    Args:
        table_name: Name of the table.
        
    Returns:
        Dict mapping FK column names to parent table names.
        Excludes EXTERNAL references.
    """
    if table_name not in SID_CONFIG:
        return {}
    
    config = SID_CONFIG[table_name]
    return {
        fk_col: parent_table
        for fk_col, parent_table in config.fk_columns.items()
        if parent_table != "EXTERNAL"
    }


def get_self_reference_fk(table_name: str) -> Optional[str]:
    """
    Get self-referencing FK column if any.
    
    Args:
        table_name: Name of the table.
        
    Returns:
        Self-reference FK column name or None.
    """
    if table_name not in SID_CONFIG:
        return None
    return SID_CONFIG[table_name].self_reference_fk


def get_pk_column(table_name: str) -> Optional[str]:
    """
    Get primary key column for a table.
    
    Args:
        table_name: Name of the table.
        
    Returns:
        PK column name or None.
    """
    if table_name not in SID_CONFIG:
        return None
    return SID_CONFIG[table_name].pk_column


def get_tables_with_self_reference() -> List[str]:
    """
    Get list of tables that have self-referencing FK columns.
    These require two-pass processing.
    
    Returns:
        List of table names with self-references.
    """
    return [
        table_name
        for table_name, config in SID_CONFIG.items()
        if config.self_reference_fk is not None
    ]
