use deltalake::arrow::record_batch::RecordBatch;
use deltalake::{DeltaTable, DeltaTableError};
use futures::TryStreamExt;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    deltalake_catalog_unity::register_handlers(None);
    deltalake_azure::register_handlers(None);
    let table_url = Url::parse("abfss://databricks@wayveproddataset.dfs.core.windows.net/delta/wayve_corpus/all_data")
        .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
    let table = DeltaTable::try_from_url_with_storage_options(
        table_url,
        std::collections::HashMap::from([
            (
                String::from("use_azure_cli"),
                String::from("true"),
            ),
        ]),
    )
    .await?;

    Ok(())
}
