use deltalake::arrow::record_batch::RecordBatch;
use deltalake::{DeltaTable, DeltaTableError};
use futures::TryStreamExt;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    deltalake_catalog_unity::register_handlers(None);
    deltalake_azure::register_handlers(None);
    let table_url = Url::parse("uc://prod_data_pipeline.wayve_corpus.all_data")
        .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
    let table = DeltaTable::try_from_url_with_storage_options(
        table_url,
        std::collections::HashMap::from([
            (
                String::from("workspace_url"),
                String::from("https://adb-7835963732836817.17.azuredatabricks.net"),
            ),
        ]),
    )
    .await?;

    let (_table, mut stream) = table.scan_table().await?;
    if let Some(batch) = stream.try_next().await? {
        let batch: RecordBatch = batch;
        println!(
            "read {} rows across {} columns from the first parquet batch",
            batch.num_rows(),
            batch.num_columns(),
        );
    } else {
        println!("table contains no data");
    }

    Ok(())
}
