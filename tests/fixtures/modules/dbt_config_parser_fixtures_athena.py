DBT_PROFILE_FIXTURE = {
    "athena": {
        "outputs": {
            "data": {
                "aws_profile_name": "data",
                "database": "awsdatacatalog",
                "num_retries": 10,
                "region_name": "eu-west-1",
                "s3_data_dir": "s3://bucket1-data-lake/path1/tmp",
                "s3_data_naming": "schema_table",
                "s3_staging_dir": "s3://bucket1-data-lake/path1/",
                "schema": "analytics_engineering",
                "threads": 4,
                "type": "athena",
                "work_group": "primary",
            },
        }
    }
}

DBT_MANIFEST_FILE_FIXTURE = {
    "nodes": {
        "model.main.model1": {
            "database": "awsdatacatalog",
            "schema": "analytics_engineering",
            "unique_id": "model.main.model1",
            "name": "model1",
            "config": {
                "external_location": "s3://bucket1-data-lake/path1/model1",
                "materialized": "incremental",
                "incremental_strategy": "insert_overwrite",
            },
            "description": "Details of revenue calculation at supplier level for each observation day",
            "tags": ["daily"],
            "unrendered_config": {
                "materialized": "incremental",
                "external_location": "s3://bucket1-data-lake/path1/model1",
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
            "schema": "analytics_engineering",
            "unique_id": "model.main.stg_core_schema1__table1",
            "name": "stg_core_schema1__table1",
            "config": {
                "materialized": "table",
                "external_location": "s3://bucket1-data-lake/path2/stg_core_schema1__table1",
            },
            "depends_on": {
                "macros": [],
                "nodes": ["source.main.core_schema1.table1"],
            },
        },
        "model.main.stg_core_schema2__table2": {
            "schema": "analytics_engineering",
            "name": "stg_core_schema2__table2",
            "unique_id": "model.main.stg_core_schema2__table2",
            "config": {
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
            "name": "model2",
            "schema": "analytics_engineering",
            "unique_id": "model.main.model2",
            "config": {
                "external_location": "s3://bucket1-data-lake/path2/model2",
                "materialized": "table",
            },
            "depends_on": {"macros": [], "nodes": []},
        },
        "model.main.int_model3": {
            "name": "int_model3",
            "unique_id": "model.main.int_model3",
            "schema": "analytics_engineering",
            "config": {
                "materialized": "ephemeral",
            },
            "depends_on": {
                "macros": [],
                "nodes": ["model.main.int_model2"],
            },
        },
        "seed.main.seed_buyer_country_overwrite": {
            "database": "awsdatacatalog",
            "schema": "analytics_engineering",
            "unique_id": "seed.main.seed_buyer_country_overwrite",
            "name": "seed_buyer_country_overwrite",
            "resource_type": "seed",
            "alias": "seed_buyer_country_overwrite",
            "tags": ["analytics"],
            "description": "",
            "created_at": 1700216177.105391,
            "depends_on": {"macros": []},
        },
        "model.main.model3": {
            "name": "model3",
            "schema": "analytics_engineering",
            "unique_id": "model.main.model3",
            "config": {
                "external_location": "s3://bucket1-data-lake/path2/model3",
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
            "schema": "analytics_engineering",
            "config": {
                "materialized": "ephemeral",
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
            "database": "awsdatacatalog",
            "schema": "core_schema1",
            "resource_type": "source",
            "unique_id": "source.main.core_schema1.table1",
            "name": "table1",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table2": {
            "source_name": "table2",
            "database": "awsdatacatalog",
            "schema": "core_schema2",
            "resource_type": "source",
            "unique_id": "source.main.core_schema2.table2",
            "name": "table2",
            "tags": ["analytics"],
            "description": "",
        },
        "source.main.core_schema2.table3": {
            "source_name": "table3",
            "database": "awsdatacatalog",
            "schema": "core_schema2",
            "resource_type": "source",
            "unique_id": "source.main.core_schema2.table3",
            "name": "table3",
            "tags": ["analytics"],
            "description": "",
        },
    },
}

EXPECTED_STAGING_NODE = [
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
        "follow_external_dependency": True,
    },
    {
        "type": "s3",
        "name": "s3_stg_core_schema1__table1",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
]

EXPECTED_SEED_NODE = [
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    }
]

EXPECTED_MODEL_MULTIPLE_DEPENDENCIES = [
    {
        "type": "dummy",
        "name": "int_model3",
    },
    {
        "type": "dummy",
        "name": "int_model2",
    },
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    },
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
        "follow_external_dependency": True,
    },
    {
        "type": "s3",
        "name": "s3_stg_core_schema1__table1",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
    {
        "type": "athena",
        "name": "analytics_engineering__model2_athena",
        "schema": "analytics_engineering",
        "table": "model2",
        "follow_external_dependency": True,
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "s3_model2",
        "path": "path2/model2",
        "type": "s3",
    },
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "stg_core_schema2__table2",
        "name": "analytics_engineering__stg_core_schema2__table2_athena",
    },
    {
        "type": "athena",
        "name": "core_schema2__table2_athena",
        "schema": "core_schema2",
        "table": "table2",
        "follow_external_dependency": True,
    },
    {
        "type": "athena",
        "name": "core_schema2__table3_athena",
        "schema": "core_schema2",
        "table": "table3",
        "follow_external_dependency": True,
    },
]

EXPECTED_EPHEMERAL_NODE = [
    {
        "type": "dummy",
        "name": "int_model3",
    },
    {
        "type": "dummy",
        "name": "int_model2",
    },
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    },
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
        "follow_external_dependency": True,
    },
    {
        "type": "s3",
        "name": "s3_stg_core_schema1__table1",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
]

EXPECTED_MODEL_NODE = [
    {
        "type": "athena",
        "name": "analytics_engineering__model1_athena",
        "schema": "analytics_engineering",
        "table": "model1",
        "follow_external_dependency": True,
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "s3_model1",
        "path": "path1/model1",
        "type": "s3",
    },
]

EXPECTED_DAGGER_INPUTS = [
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "stg_core_schema2__table2",
        "name": "analytics_engineering__stg_core_schema2__table2_athena",
    },
    {
        "type": "athena",
        "name": "core_schema2__table2_athena",
        "schema": "core_schema2",
        "table": "table2",
        "follow_external_dependency": True,
    },
    {
        "type": "athena",
        "name": "core_schema2__table3_athena",
        "schema": "core_schema2",
        "table": "table3",
        "follow_external_dependency": True,
    },
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    },
    {
        "name": "analytics_engineering__model2_athena",
        "schema": "analytics_engineering",
        "table": "model2",
        "type": "athena",
        "follow_external_dependency": True,
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "s3_model2",
        "path": "path2/model2",
        "type": "s3",
    },
    {
        "type": "dummy",
        "name": "int_model3",
    },
    {
        "type": "dummy",
        "name": "int_model2",
    },
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
        "follow_external_dependency": True,
    },
    {
        "type": "s3",
        "name": "s3_stg_core_schema1__table1",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
]

EXPECTED_DBT_STAGING_MODEL_DAGGER_INPUTS = [
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
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    }
]

EXPECTED_DAGGER_OUTPUTS = [
    {
        "name": "analytics_engineering__model1_athena",
        "schema": "analytics_engineering",
        "table": "model1",
        "type": "athena",
    },
    {
        "bucket": "bucket1-data-lake",
        "name": "output_s3_path",
        "path": "path1/model1",
        "type": "s3",
    },
]

EXPECTED_DBT_STAGING_MODEL_DAGGER_OUTPUTS = [
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
    },
    {
        "type": "s3",
        "name": "output_s3_path",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
]

EXPECTED_DBT_INT_MODEL_DAGGER_INPUTS = [
    {
        "type": "athena",
        "schema": "analytics_engineering",
        "table": "seed_buyer_country_overwrite",
        "name": "analytics_engineering__seed_buyer_country_overwrite_athena",
    },
    {
        "name": "analytics_engineering__stg_core_schema1__table1_athena",
        "type": "athena",
        "table": "stg_core_schema1__table1",
        "schema": "analytics_engineering",
        "follow_external_dependency": True,
    },
    {
        "type": "s3",
        "name": "s3_stg_core_schema1__table1",
        "bucket": "bucket1-data-lake",
        "path": "path2/stg_core_schema1__table1",
    },
]
