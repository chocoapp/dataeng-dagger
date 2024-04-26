DATABRICKS_DBT_PROFILE_FIXTURE = {
    "databricks": {
        "outputs": {
            "data": {
                "type": "databricks",
                "catalog": "hive_metastore",
                "schema": "analytics_engineering",
                "host": "xxx.databricks.com",
                "http_path": "/sql/1.0/warehouses/xxx",
                "token": "{{ env_var('SECRETDATABRICKS') }}"
            },
        }

    }
}

DATABRICKS_DBT_MANIFEST_FILE_FIXTURE = {
    "nodes": {
        "model.main.model1": {
            "database": "marts",
            "schema": "analytics_engineering",
            "name": "model1",
            "unique_id": "model.main.model1",
            "resource_type": "model",
            "config": {
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/marts",
                "materialized": "incremental",
                "incremental_strategy": "insert_overwrite",
            },
            "description": "Details of revenue calculation at supplier level for each observation day",
            "tags": ["daily"],
            "unrendered_config": {
                "materialized": "incremental",
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/marts",
                "incremental_strategy": "insert_overwrite",
                "partitioned_by": ["year", "month", "day", "dt"],
                "tags": ["daily"],
                "on_schema_change": "fail",
            },
            "depends_on": {
                "macros": [
                    "macro.main.macro1",
                    "macro.main.macro2",
                ],
                "nodes": [
                    "model.main.stg_core_schema2__table2",
                    "model.main.model2",
                    "model.main.int_model3",
                    "seed.main.seed_buyer_country_overwrite",
                ],
            },
        },
        "model.main.stg_core_schema1__table1": {
            "database": "hive_metastore",
            "schema": "data_preparation",
            "name": "stg_core_schema1__table1",
            "unique_id": "model.main.stg_core_schema1__table1",
            "resource_type": "model",
            "config": {
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/preparation",
                "materialized": "view",
            },
            "depends_on": {
                "macros": [],
                "nodes": ["source.main.core_schema1.table1"],
            },
        },
        "model.main.stg_core_schema2__table2": {
            "database": "hive_metastore",
            "schema": "data_preparation",
            "name": "stg_core_schema2__table2",
            "unique_id": "model.main.stg_core_schema2__table2",
            "resource_type": "model",
            "config": {
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/preparation",
                "materialized": "view",
            },
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.main.core_schema2.table2",
                    "source.main.core_schema2.table3",
                    "seed.main.seed_buyer_country_overwrite",
                ],
            },
        },
        "model.main.model2": {
            "database": "marts",
            "schema": "analytics_engineering",
            "name": "model2",
            "unique_id": "model.main.model2",
            "resource_type": "model",
            "config": {
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/marts",
                "materialized": "table",
            },
            "depends_on": {"macros": [], "nodes": []},
        },
        "model.main.int_model3": {
            "name": "int_model3",
            "unique_id": "model.main.int_model3",
            "database": "intermediate",
            "schema": "analytics_engineering",
            "resource_type": "model",
            "config": {
                "materialized": "ephemeral",
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/intermediate",
            },
            "depends_on": {
                "macros": [],
                "nodes": ["model.main.int_model2"],
            },
        },
        "seed.main.seed_buyer_country_overwrite": {
            "database": "hive_metastore",
            "schema": "datastg_preparation",
            "name": "seed_buyer_country_overwrite",
            "unique_id": "seed.main.seed_buyer_country_overwrite",
            "resource_type": "seed",
            "alias": "seed_buyer_country_overwrite",
            "tags": ["analytics"],
            "description": "",
            "created_at": 1700216177.105391,
            "depends_on": {"macros": []},
        },
        "model.main.model3": {
            "name": "model3",
            "database": "marts",
            "schema": "analytics_engineering",
            "unique_id": "model.main.model3",
            "config": {
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/marts",
            },
            "depends_on": {
                "macros": [],
                "nodes": [
                    "model.main.int_model3",
                    "model.main.model2",
                    "seed.main.seed_buyer_country_overwrite",
                    "model.main.stg_core_schema2__table2",
                ],
            },
        },
        "model.main.int_model2": {
            "name": "int_model2",
            "unique_id": "model.main.int_model2",
            "database": "intermediate",
            "schema": "analytics_engineering",
            "config": {
                "materialized": "ephemeral",
                "location_root": "s3://chodata-data-lake/analytics_warehouse/data/intermediate",
            },
            "depends_on": {
                "macros": [],
                "nodes": [
                    "seed.main.seed_buyer_country_overwrite",
                    "model.main.stg_core_schema1__table1",
                ],
            },
        },
    },
    "sources": {
        "source.main.core_schema1.table1": {
            "source_name": "table1",
            "database": "hive_metastore",
            "schema": "core_schema1",
            "resource_type": "source",
            "unique_id": "source.main.core_schema1.table1",
            "name": "table1",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table2": {
            "source_name": "table2",
            "database": "hive_metastore",
            "schema": "core_schema2",
            "resource_type": "source",
            "unique_id": "source.main.core_schema2.table2",
            "name": "table2",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table3": {
            "source_name": "table3",
            "database": "hive_metastore",
            "schema": "core_schema2",
            "resource_type": "source",
            "unique_id": "source.main.core_schema2.table3",
            "name": "table3",
            "tags": ["analytics"],
            "description": "",
        },
    },
}

DATABRICKS_EXPECTED_STAGING_NODE = [
    {
        "name": "stg_core_schema1__table1",
        "type": "dummy",
        "follow_external_dependency": True,
    },
]

DATABRICKS_EXPECTED_SEED_NODE = [
    {
        "type": "dummy",
        "name": "seed_buyer_country_overwrite",
    }
]

DATABRICKS_EXPECTED_MODEL_MULTIPLE_DEPENDENCIES = [
    {
        "type": "dummy",
        "name": "int_model3",
        "follow_external_dependency": True,
    },
    {
        "type": "dummy",
        "name": "int_model2",
        "follow_external_dependency": True,
    },
    {
        "type": "dummy",
        "name": "seed_buyer_country_overwrite",
    },
    {
        "name": "stg_core_schema1__table1",
        "type": "dummy",
        "follow_external_dependency": True,
    },
    {
        "type": "databricks",
        "name": "marts__analytics_engineering__model2_databricks",
        "catalog": "marts",
        "schema": "analytics_engineering",
        "table": "model2",
        "follow_external_dependency": True,
    },
    {
        "bucket": "chodata-data-lake",
        "name": "marts__analytics_engineering__model2_s3",
        "path": "analytics_warehouse/data/marts/analytics_engineering/model2",
        "type": "s3",
    },
    {
        "name": "stg_core_schema2__table2",
        "type": "dummy",
        "follow_external_dependency": True,
    },
]

DATABRICKS_EXPECTED_EPHEMERAL_NODE = [
    {
        "type": "dummy",
        "name": "int_model3",
        "follow_external_dependency": True,
    },
    {
        "type": "dummy",
        "name": "int_model2",
        "follow_external_dependency": True,
    },
    {
        "type": "dummy",
        "name": "seed_buyer_country_overwrite",
    },
    {
        "name": "stg_core_schema1__table1",
        "type": "dummy",
        "follow_external_dependency": True,
    }
]

DATABRICKS_EXPECTED_MODEL_NODE = [
    {
        "type": "databricks",
        "name": "marts__analytics_engineering__model1_databricks",
        "catalog": "marts",
        "schema": "analytics_engineering",
        "table": "model1",
        "follow_external_dependency": True,
    },
    {
        "bucket": "chodata-data-lake",
        "name": "marts__analytics_engineering__model1_s3",
        "path": "analytics_warehouse/data/marts/analytics_engineering/model1",
        "type": "s3",
    },
]

DATABRICKS_EXPECTED_DAGGER_INPUTS = [
    {
        "name": "stg_core_schema2__table2",
        "type": "dummy",
        "follow_external_dependency": True,
    },
    {
        "name": "marts__analytics_engineering__model2_databricks",
        "catalog": "marts",
        "schema": "analytics_engineering",
        "table": "model2",
        "type": "databricks",
        "follow_external_dependency": True,
    },
    {
        "bucket": "chodata-data-lake",
        "name": "marts__analytics_engineering__model2_s3",
        "path": "analytics_warehouse/data/marts/analytics_engineering/model2",
        "type": "s3",
    },
    {
        "type": "dummy",
        "name": "int_model3",
        "follow_external_dependency": True,
    },
    {
        "type": "dummy",
        "name": "int_model2",
        "follow_external_dependency": True,
    },
    {"name": "seed_buyer_country_overwrite", "type": "dummy"},
    {
        "name": "stg_core_schema1__table1",
        "type": "dummy",
        "follow_external_dependency": True,
    },
]

DATABRICKS_EXPECTED_DBT_STAGING_MODEL_DAGGER_INPUTS = [
    {
        "follow_external_dependency": True,
        "name": "core_schema2__table2_athena",
        "schema": "core_schema2",
        "table": "table2",
        "type": "athena",
    },
    {
        "follow_external_dependency": True,
        "name": "core_schema2__table3_athena",
        "schema": "core_schema2",
        "table": "table3",
        "type": "athena",
    },
    {"name": "seed_buyer_country_overwrite", "type": "dummy"},
]

DATABRICKS_EXPECTED_DBT_INT_MODEL_DAGGER_INPUTS = [
    {"name": "seed_buyer_country_overwrite", "type": "dummy"},
    {
        "name": "stg_core_schema1__table1",
        "type": "dummy",
        "follow_external_dependency": True,
    },
]

DATABRICKS_EXPECTED_DAGGER_OUTPUTS = [
    {
        "name": "marts__analytics_engineering__model1_databricks",
        "catalog": "marts",
        "schema": "analytics_engineering",
        "table": "model1",
        "type": "databricks",
    },
    {
        "bucket": "chodata-data-lake",
        "name": "marts__analytics_engineering__model1_s3",
        "path": "analytics_warehouse/data/marts/analytics_engineering/model1",
        "type": "s3",
    },
    {
        "name": "analytics_engineering__model1_athena",
        "schema": "analytics_engineering",
        "table": "model1",
        "type": "athena",
    }
]

DATABRICKS_EXPECTED_DBT_STAGING_MODEL_DAGGER_OUTPUTS = [
    {
        "type": "dummy",
        "name": "stg_core_schema2__table2",
    },
]


