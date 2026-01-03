# Real-Time Job Progress Tracker

A modern, production-ready demonstration of WebSocket-based real-time progress tracking for background jobs using Rust, Poem, and Tokio.

## Information

This project is over 4 years old, packages have been updated, but it still runs and builds. The codebase is not actively maintained, but it can still serve as a reference for implementing similar functionality.

## Features

- **Real-Time Progress Updates**: WebSocket-powered live progress tracking with sub-second updates
- **Multiple Job Types**: Four different job simulations showcasing various processing patterns:
  - Simple Counter: Basic incremental progress tracking
  - File Processing: Simulates processing multiple files with varying durations
  - Data Aggregation: Multi-step pipeline with data fetching and analysis
  - Batch Operation: Queue-based batch processing with variable timing
- **Modern UI**: Responsive design with animations, status indicators, and visual feedback
- **Job History**: Persistent job tracking using browser localStorage
- **Clean Architecture**: Thread-safe state management with Arc and Mutex
- **Docker Ready**: Containerized deployment for easy demonstration

## Tech Stack

- **Rust**: Safe, concurrent systems programming
- **Poem**: Modern async web framework
- **Tokio**: Async runtime with broadcast channels for pub/sub messaging
- **WebSockets**: Real-time bidirectional communication
- **Tera**: Template engine for server-side rendering
- **Tailwind CSS**: Utility-first CSS framework

## Quick Start

### Using Docker (Recommended)

```bash
# Build and run with docker-compose
docker-compose up --build

# Or build and run manually
docker build -t job-tracker .
docker run -p 3000:3000 job-tracker
```

Visit `http://localhost:3000` in your browser.

### Running Locally

**Prerequisites:**
- Rust 1.75 or later
- Cargo

```bash
# Clone the repository
git clone <repository-url>
cd congenial-meme

# Run the application
cargo run

# Run tests
cargo test
```

Visit `http://localhost:3000` in your browser.

### Configuration

- `BIND_ADDR` (default: `127.0.0.1:3000`): address the server listens on.
  - For Docker/remote access, use `0.0.0.0:3000`.

## Architecture

### Core Design Pattern

The application demonstrates cross-thread communication using Tokio's broadcast channels to track progress of spawned background tasks.

```rust
struct AppState {
    clients: Mutex<HashMap<String, Sender<String>>>,
}
```

**Key Components:**

1. **Shared State**: `Arc<AppState>` allows multiple handlers to access the same client registry
   - `Arc` (Atomic Reference Counted) enables safe sharing across threads
   - `Mutex` provides interior mutability for thread-safe HashMap access

2. **Broadcast Channels**: Each job gets a unique `broadcast::channel` for publishing progress
   - Sender stored in HashMap with UUID key
   - Multiple receivers can subscribe (WebSocket connections)

3. **WebSocket Upgrade**: HTTP requests upgrade to WebSocket for bidirectional streaming
   - Client connects to `/ws/:id` with their unique job ID
   - Server sends progress updates as jobs execute

### Request Flow

```
1. User visits dashboard → GET /
2. User selects job type → GET /job?job_type=<type>
3. Server creates job:
   - Generate UUID
   - Create broadcast channel
   - Spawn async task
   - Store sender in AppState
   - Return page with job ID
4. Browser connects WebSocket → WS /ws/:id
5. Background task sends progress → Channel broadcasts
6. WebSocket receives updates → UI updates in real-time
```

### Why Arc?

`Arc<AppState>` is required because:
- Poem's `AddData` middleware clones data for each request handler
- Multiple concurrent requests need access to the same HashMap
- Arc provides cheap cloning (just incrementing a reference count)
- Ensures all handlers share the same underlying state

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Dashboard with job type selection |
| `/job?job_type=<type>` | GET | Start a job and view progress page |
| `/ws/:id` | WebSocket | Real-time progress updates for job |

**Job Types:**
- `counter` - Simple Counter (default)
- `file` - File Processing
- `data` - Data Aggregation
- `batch` - Batch Operation

## Testing

The project includes comprehensive tests for:
- All four job types and their completion
- Job type parsing and naming
- Shared state management
- Channel communication

```bash
cargo test
```

## Project Structure

```
.
├── src/
│   └── main.rs          # Main application code
├── templates/
│   ├── dashboard.html.tera   # Job selection dashboard
│   └── index.html.tera       # Job progress tracking page
├── Cargo.toml           # Rust dependencies
├── Dockerfile           # Container build configuration
├── docker-compose.yml   # Docker orchestration
└── readme.md           # This file
```

## Future Enhancements

- [ ] Job cancellation support
- [ ] Persistent job storage (PostgreSQL/SQLite)
- [ ] Authentication and authorization
- [ ] Job queue with priority scheduling
- [ ] Metrics and observability (Prometheus)
- [ ] Multiple concurrent jobs per user
- [ ] Job result artifacts (file downloads)

## Use Cases

This pattern is ideal for:
- File upload and processing pipelines
- Data import/export operations
- Report generation
- Batch email/notification sending
- Image/video processing
- Database migrations
- Long-running API aggregations

## Performance

- Handles multiple concurrent jobs efficiently
- WebSocket connections are lightweight
- Broadcast channels allow multiple subscribers per job
- Async/await enables high concurrency with low overhead

## Contributing

This is a portfolio/demonstration project, but suggestions and improvements are welcome!

## License

MIT

## Acknowledgments

- [Tokio Shared State Tutorial](https://tokio.rs/tokio/tutorial/shared-state)
- [Poem Web Framework Examples](https://github.com/poem-web/poem/tree/master/examples)
- Rust community for excellent async ecosystem
