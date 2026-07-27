use deltalake::{DeltaTableBuilder, DeltaTableError};

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    deltalake_azure::register_handlers(None);
    let table_uri =
        "abfss://databricks@wayveproddataset.dfs.core.windows.net/delta/wayve_corpus/all_data";

    let _table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(std::collections::HashMap::from([(
            String::from("use_azure_cli"),
            String::from("true"),
        )]))
        .load()
        .await?;

    Ok(())
}
