from cdw_extract import preview

result = preview(
    connection_id="19f7858e-d65a-40a3-8c11-f4d542e1254f",
    data_root="/Users/root1/cdw",
    request={
        "sourceType": "table",
        "tableName": "cdw_itg_diag_inf",
        "schemaName": "testdb",
        "limit": 5,
    },
)

print(result)
