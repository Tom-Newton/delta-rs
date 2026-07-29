use deltalake::{DeltaTableBuilder, DeltaTableError};

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    deltalake_catalog_unity::register_handlers(None);
    deltalake_azure::register_handlers(None);
    let table_uri =
        "uc://prod_data_pipeline.wayve_corpus.all_data";

    let _table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(std::collections::HashMap::from([(
            String::from("unity_workspace_url"),
            String::from("https://adb-7835963732836817.17.azuredatabricks.net"),
        ), (
            String::from("use_azure_cli"),
            String::from("true"),
        )]))
        .load()
        .await?;

    Ok(())
}
