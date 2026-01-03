use futures_util::{SinkExt, StreamExt};
use poem::{
    error::InternalServerError,
    get, handler,
    listener::TcpListener,
    middleware::{AddData, Tracing},
    web::{
        websocket::{Message, WebSocket},
        Data, Html, Path, Query,
    },
    EndpointExt, IntoResponse, Route, Server,
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tera::{Context, Tera};
use tokio::sync::broadcast::Sender;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref TEMPLATES: Tera = {
        let mut tera = match Tera::new("templates/**/*") {
            Ok(t) => t,
            Err(e) => {
                println!("Parsing error(s): {}", e);
                ::std::process::exit(1);
            }
        };
        tera.autoescape_on(vec![".html", ".sql"]);
        tera
    };
}

#[derive(Debug, Clone)]
enum JobType {
    SimpleCounter,
    FileProcessing,
    DataAggregation,
    BatchOperation,
}

impl JobType {
    fn from_str(s: &str) -> Self {
        match s {
            "file" | "file-processing" => JobType::FileProcessing,
            "data" | "data-aggregation" => JobType::DataAggregation,
            "batch" | "batch-operation" => JobType::BatchOperation,
            "counter" | "simple-counter" => JobType::SimpleCounter,
            _ => JobType::SimpleCounter,
        }
    }

    fn name(&self) -> &str {
        match self {
            JobType::SimpleCounter => "Simple Counter",
            JobType::FileProcessing => "File Processing",
            JobType::DataAggregation => "Data Aggregation",
            JobType::BatchOperation => "Batch Operation",
        }
    }
}

struct AppState {
    clients: Mutex<HashMap<String, Sender<String>>>,
}

async fn simple_counter(channel: Sender<String>) {
    for i in 1..=10 {
        let progress = i * 10;
        let _ = channel.send(progress.to_string());
        sleep(Duration::from_secs(1)).await;
    }
}

async fn file_processing(channel: Sender<String>) {
    let files = vec!["data.csv", "images.zip", "report.pdf", "config.json", "logs.txt"];
    let total_files = files.len();

    for (idx, file) in files.iter().enumerate() {
        let progress = ((idx + 1) * 100) / total_files;
        let _ = channel.send(progress.to_string());
        // Simulate varying processing times for different files
        let delay = match file {
            f if f.ends_with(".zip") => 3,
            f if f.ends_with(".pdf") => 2,
            _ => 1,
        };
        sleep(Duration::from_secs(delay)).await;
    }
}

async fn data_aggregation(channel: Sender<String>) {
    let steps = vec![
        ("Fetching user data", 15),
        ("Fetching analytics", 30),
        ("Fetching transactions", 50),
        ("Computing metrics", 70),
        ("Generating insights", 85),
        ("Finalizing report", 100),
    ];

    for (_, progress) in steps.iter() {
        let _ = channel.send(progress.to_string());
        sleep(Duration::from_millis(1500)).await;
    }
}

async fn batch_operation(channel: Sender<String>) {
    let batch_size = 20;

    for i in 1..=batch_size {
        let progress = (i * 100) / batch_size;
        let _ = channel.send(progress.to_string());
        // Simulate variable processing time
        let delay = if i % 5 == 0 { 800 } else { 400 };
        sleep(Duration::from_millis(delay)).await;
    }
}

async fn process(channel: Sender<String>, job_type: JobType) {
    match job_type {
        JobType::SimpleCounter => simple_counter(channel).await,
        JobType::FileProcessing => file_processing(channel).await,
        JobType::DataAggregation => data_aggregation(channel).await,
        JobType::BatchOperation => batch_operation(channel).await,
    }
}

#[derive(Deserialize)]
struct JobQuery {
    #[serde(default)]
    job_type: String,
    #[serde(default)]
    #[serde(alias = "type")]
    r#type: String,
}

#[handler]
fn job(state: Data<&Arc<AppState>>, Query(query): Query<JobQuery>) -> Result<Html<String>, poem::Error> {
    let mut s = state.clients.lock().unwrap();
    let id = Uuid::new_v4();
    let sender = tokio::sync::broadcast::channel::<String>(32).0;
    let sender_worker = sender.clone();
    s.insert(id.to_string(), sender);

    let job_type_raw = if !query.r#type.is_empty() {
        query.r#type.as_str()
    } else {
        query.job_type.as_str()
    };
    let job_type = JobType::from_str(job_type_raw);
    let job_name = job_type.name().to_string();

    tokio::spawn(async move {
        process(sender_worker, job_type).await;
    });

    let mut context = Context::new();
    context.insert("id", &id.to_string());
    context.insert("job_type", &job_name);
    TEMPLATES
        .render("job.html.tera", &context)
        .map_err(InternalServerError)
        .map(Html)
}

#[handler]
fn index() -> Result<Html<String>, poem::Error> {
    let context = Context::new();
    TEMPLATES
        .render("index.html.tera", &context)
        .map_err(InternalServerError)
        .map(Html)
}

#[handler]
fn ws(Path(name): Path<String>, ws: WebSocket, state: Data<&Arc<AppState>>) -> impl IntoResponse {
    let client = state.clients.lock().unwrap();
    let sender = client.get(&name).unwrap().clone();
    let mut receiver = sender.subscribe();
    ws.on_upgrade(move |socket| async move {
        let (mut sink, mut stream) = socket.split();

        tokio::spawn(async move {
            while let Some(Ok(msg)) = stream.next().await {
                if let Message::Text(text) = msg {
                    if sender.send(format!("{}: {}", name, text)).is_err() {
                        break;
                    }
                }
            }
        });

        tokio::spawn(async move {
            while let Ok(msg) = receiver.recv().await {
                if sink.send(Message::Text(msg)).await.is_err() {
                    break;
                }
            }
        });
    })
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "poem=debug");
    }
    tracing_subscriber::fmt::init();

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    let state = Arc::new(AppState {
        clients: Mutex::new(HashMap::new()),
    });

    let app = Route::new()
        .at("/", get(index))
        .at("/job", get(job))
        .at("/ws/:id", get(ws))
        .with(AddData::new(state))
        .with(Tracing);

    Server::new(TcpListener::bind(bind_addr))
        .run(app)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_simple_counter_completes() {
        let (sender, mut receiver) = broadcast::channel::<String>(32);

        let handle = tokio::spawn(async move {
            simple_counter(sender).await;
        });

        let mut progress_values = Vec::new();
        loop {
            match timeout(Duration::from_millis(2000), receiver.recv()).await {
                Ok(Ok(value)) => {
                    let parsed = value.parse::<i32>().unwrap();
                    progress_values.push(parsed);
                    if parsed >= 100 {
                        break;
                    }
                }
                Ok(Err(_)) => break, // Channel closed
                Err(_) => break, // Timeout
            }
        }

        // Wait for task to complete
        let _ = timeout(Duration::from_secs(2), handle).await;

        assert_eq!(progress_values.len(), 10);
        assert_eq!(*progress_values.last().unwrap(), 100);
        assert_eq!(progress_values[0], 10);
    }

    #[tokio::test]
    async fn test_file_processing_completes() {
        let (sender, mut receiver) = broadcast::channel::<String>(32);

        let handle = tokio::spawn(async move {
            file_processing(sender).await;
        });

        let mut progress_values = Vec::new();
        loop {
            match timeout(Duration::from_millis(4000), receiver.recv()).await {
                Ok(Ok(value)) => {
                    let parsed = value.parse::<i32>().unwrap();
                    progress_values.push(parsed);
                    if parsed >= 100 {
                        break;
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        let _ = timeout(Duration::from_secs(2), handle).await;

        assert_eq!(progress_values.len(), 5); // 5 files
        assert_eq!(*progress_values.last().unwrap(), 100);
    }

    #[tokio::test]
    async fn test_data_aggregation_completes() {
        let (sender, mut receiver) = broadcast::channel::<String>(32);

        let handle = tokio::spawn(async move {
            data_aggregation(sender).await;
        });

        let mut progress_values = Vec::new();
        loop {
            match timeout(Duration::from_millis(2000), receiver.recv()).await {
                Ok(Ok(value)) => {
                    let parsed = value.parse::<i32>().unwrap();
                    progress_values.push(parsed);
                    if parsed >= 100 {
                        break;
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        let _ = timeout(Duration::from_secs(2), handle).await;

        assert_eq!(progress_values.len(), 6); // 6 steps
        assert_eq!(*progress_values.last().unwrap(), 100);
        assert_eq!(progress_values[0], 15); // First step
    }

    #[tokio::test]
    async fn test_batch_operation_completes() {
        let (sender, mut receiver) = broadcast::channel::<String>(32);

        let handle = tokio::spawn(async move {
            batch_operation(sender).await;
        });

        let mut progress_values = Vec::new();
        loop {
            match timeout(Duration::from_millis(1000), receiver.recv()).await {
                Ok(Ok(value)) => {
                    let parsed = value.parse::<i32>().unwrap();
                    progress_values.push(parsed);
                    if parsed >= 100 {
                        break;
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => break,
            }
        }

        let _ = timeout(Duration::from_secs(2), handle).await;

        assert_eq!(progress_values.len(), 20); // 20 batches
        assert_eq!(*progress_values.last().unwrap(), 100);
    }

    #[test]
    fn test_job_type_from_str() {
        assert!(matches!(JobType::from_str("file"), JobType::FileProcessing));
        assert!(matches!(JobType::from_str("data"), JobType::DataAggregation));
        assert!(matches!(JobType::from_str("batch"), JobType::BatchOperation));
        assert!(matches!(JobType::from_str("counter"), JobType::SimpleCounter));
        assert!(matches!(JobType::from_str("unknown"), JobType::SimpleCounter));
    }

    #[test]
    fn test_job_type_names() {
        assert_eq!(JobType::SimpleCounter.name(), "Simple Counter");
        assert_eq!(JobType::FileProcessing.name(), "File Processing");
        assert_eq!(JobType::DataAggregation.name(), "Data Aggregation");
        assert_eq!(JobType::BatchOperation.name(), "Batch Operation");
    }

    #[tokio::test]
    async fn test_app_state_can_store_clients() {
        let state = Arc::new(AppState {
            clients: Mutex::new(HashMap::new()),
        });

        let id = Uuid::new_v4().to_string();
        let (sender, _) = broadcast::channel::<String>(32);

        {
            let mut clients = state.clients.lock().unwrap();
            clients.insert(id.clone(), sender);
        }

        let clients = state.clients.lock().unwrap();
        assert!(clients.contains_key(&id));
    }
}
