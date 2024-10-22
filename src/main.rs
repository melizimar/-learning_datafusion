use std::error::Error;

use datafusion::arrow::util::pretty;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let input = "./input/base-50K.csv";

    let ctx = SessionContext::new();

    let opts = CsvReadOptions::new()
        .delimiter(b';')
        .has_header(true)
        .file_extension("csv");

    ctx.register_csv("base", input, opts).await?;

    // Captura os headers/cabeçalhos do arquivo csv
    let headers = get_headers(input).await?;

    // Adiciona aspas simples em cada coluna e junta com vírgula
    let headers_ = headers
        .iter()
        .map(|col| format!("\"{}\"", col)) // Adiciona aspas simples em cada coluna
        .collect::<Vec<String>>()
        .join(", ");

    let query_df = format!(
        r#"
        SELECT 
            TRANSLATE (
                nome,
                ŠŽšžŸÁÇÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕËÜÏÖÑÝåáçéíóúàèìòùâêîôûãõëüïöñýÿ',
                'SZszYACEIOUAEIOUAEIOUAOEUIONYaaceiouaeiouaeiouaoeuionyy'
            ) as n,
            {}
        FROM BASE
        LIMIT {} 
        OFFSET {}
    "#,
        headers_,
        3,  // limit = 3
        10  // offset = 3
    );

    let df = ctx.sql(&query_df).await?;

    // execute the plan
    let results = df.collect().await?;

    // format the results
    let pretty_results = pretty::pretty_format_batches(&results)?.to_string();

    println!("{}", pretty_results);
    Ok(())
}

async fn get_headers(input: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let ctx = SessionContext::new();

    let opts = CsvReadOptions::new()
        .delimiter(b';')
        .has_header(true)
        .file_extension("csv");

    let df = ctx.read_csv(input, opts).await?;

    // Capturar o schema (esquema) do DataFrame
    let schema = df.schema();

    // Extrair o nome das colunas para uma variável
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone()) // Pega os nomes das colunas
        .collect();

    Ok(columns)
}
