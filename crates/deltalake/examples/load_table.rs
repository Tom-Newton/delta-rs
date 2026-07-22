use deltalake::{DeltaTable, DeltaTableError};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    deltalake_catalog_unity::register_handlers(None);
    deltalake_azure::register_handlers(None);
    let table_url = Url::parse("uc://prod_data_pipeline.wayve_corpus.all_data")
        .map_err(|e| DeltaTableError::InvalidTableLocation(e.to_string()))?;
    let ops = DeltaTable::try_from_url_with_storage_options(
        table_url,
        std::collections::HashMap::from([
            (
                String::from("workspace_url"),
                String::from("https://adb-7835963732836817.17.azuredatabricks.net"),
            ),
            (
                String::from("authority_id"),
                String::from("134c99bd-21f6-453d-9000-0ddb0758f035"),
            ),
            (
                String::from("client_id"),
                String::from("19732e5c-f2d7-4578-9cec-9d402da6a39e"),
            ),
        ]),
    )
    .await?;

    for file in ops.get_file_uris()? {
        println!("{file}");
    }
    Ok(())
}
