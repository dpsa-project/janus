
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use janus_core::time::{Clock, RealClock};
use janus_aggregator::{datastore::{Datastore, self}, task::Task, config::CommonConfig, binary_utils::{database_pool, datastore, CommonBinaryOptions}};
use tokio::fs;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
};

fn main() {
    println!("Hello, world!");
}

async fn provision_new_task(
    config: CommonConfig,
    common_options: CommonBinaryOptions,
    tasks: Vec<Task>,
) -> Result<()>
{
    // let kube_client = kube::Client::try_default()
    //     .await
    //     .context("couldn't connect to Kubernetes environment")?;
    // let config: Config = todo!(); // read_config(common_options)?;
    // install_tracing_and_metrics_handlers(config.common_config())?;
    let pool = database_pool(
        &config.database,
        common_options.database_password.as_deref(),
    ) .await?;

    // get keys from the binary options
    let datastore_keys = common_options.datastore_keys;

    let datastore = datastore(
        pool,
        RealClock::default(),
        &datastore_keys,
        // &kubernetes_secret_options
        //     .datastore_keys(common_options, kube_client)
        //     .await?,
    )?;

    provision_tasks(&datastore, tasks).await
}


async fn provision_tasks<C: Clock>(datastore: &Datastore<C>, tasks: Vec<Task>) -> Result<()> {
    // Read tasks file.
    // info!("Reading tasks file");
    // let tasks: Vec<Task> = {
    //     let task_file_contents = fs::read_to_string(tasks_file)
    //         .await
    //         .with_context(|| format!("couldn't read tasks file {:?}", tasks_file))?;
    //     serde_yaml::from_str(&task_file_contents)
    //         .with_context(|| format!("couldn't parse tasks file {:?}", tasks_file))?
    // };

    // Write all tasks requested.
    let tasks = Arc::new(tasks);
    // info!(task_count = %tasks.len(), "Writing tasks");
    datastore
        .run_tx(|tx| {
            let tasks = Arc::clone(&tasks);
            Box::pin(async move {
                for task in tasks.iter() {
                    // We attempt to delete the task, but ignore "task not found" errors since
                    // the task not existing is an OK outcome too.
                    match tx.delete_task(task.id()).await {
                        Ok(_) | Err(datastore::Error::MutationTargetNotFound) => (),
                        err => err?,
                    }

                    tx.put_task(task).await?;
                }
                Ok(())
            })
        })
        .await
        .context("couldn't write tasks")
}





