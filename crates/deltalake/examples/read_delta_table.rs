use deltalake::DeltaTableBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    deltalake::azure::register_handlers(None);

    let table_path = "abfss://databricks@wayveproddataset.dfs.core.windows.net/delta/inferred__metadata/corrected_standardised_runtime_vehicle_calibration";
    let storage_options =
        std::collections::HashMap::from([(String::from("use_azure_cli"), String::from("true"))]);

    for i in 0..500 {
        for version in 50480..50500 {
            println!("i={i} version={version}");
            let table = DeltaTableBuilder::from_valid_uri(table_path)?
                .with_storage_options(storage_options.clone())
                .with_version(version)
                .load()
                .await?;
            println!("{table}");
        }
    }

    Ok(())
}
