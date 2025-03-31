#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    deltalake::azure::register_handlers(None);

    let table_path = "abfss://databricks@wayveproddataset.dfs.core.windows.net/delta/inferred__metadata/corrected_standardised_runtime_vehicle_calibration";
    let storage_options =
        std::collections::HashMap::from([(String::from("use_azure_cli"), String::from("true"))]);
    let table = deltalake::open_table_with_storage_options(table_path, storage_options).await?;
    println!("{table}");
    Ok(())
}
