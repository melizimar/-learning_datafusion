use datafusion::prelude::*;
use datafusion::arrow::util::pretty;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    let ctx = SessionContext::new();

    let input = "./input/base-50K.csv";

    let opts = CsvReadOptions::new()
        .delimiter(b';')
        .has_header(true)
        .file_extension("csv");

    ctx.register_csv("base", input, opts).await?;

    let query = r#"
        SELECT 
            *
        FROM base
        LIMIT 10
    "#;

    let df = ctx.sql(query).await?;

    // Captura os headers/cabeçalhos do arquivo csv
    let headers = get_headers(&df);

    // execute the plan
    let results = df.collect().await?;

    // format the results
    let pretty_results = pretty::pretty_format_batches(&results)?
        .to_string();

    println!("{:?}", headers);

    Ok(())
}

fn get_headers(df: &DataFrame) -> Vec<String> {
    // Capturar o schema (esquema) do DataFrame
    let schema = df.schema();

    // Extrair o nome das colunas para uma variável
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone()) // Pegar os nomes das colunas
        .collect();

    columns
}
