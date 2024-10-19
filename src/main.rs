use datafusion::prelude::*;
use datafusion::arrow::util::pretty;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    let ctx = SessionContext::new();

    let input = "./input/base-1.csv";

    let opts = CsvReadOptions::new()
        .delimiter(b';')
        .has_header(true)
        .file_extension("csv");

    ctx.register_csv("table", input, opts).await?;

    let query = "SELECT nome FROM table";

    let df = ctx.sql(query).await?;

    // execute the plan
    let results = df.collect().await?;

    // format the results
    let pretty_results = pretty::pretty_format_batches(&results)?
        .to_string();


    println!("{}", pretty_results);
    
    Ok(())
}
