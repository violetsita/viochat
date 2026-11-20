use std::{
    collections::HashMap,
    error::Error,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use rand::distr::{Alphanumeric, SampleString};
use tokio::sync::{broadcast, Mutex};

const ROOM_CODE_LENGTH: usize = 25;
const ROOM_TTL_SECONDS: u64 = 24 * 60 * 60;
const ROOM_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct AppState {
    rooms: Arc<Mutex<HashMap<String, Arc<Room>>>>,
    started_at: Instant,
}

impl AppState {
    fn elapsed_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }
}

struct Room {
    messages: broadcast::Sender<String>,
    active_connections: AtomicUsize,
    last_used_seconds: AtomicU64,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientMessage {
    client_id: String,
    nickname: String,
    text: String,
}

impl Room {
    fn is_expired(&self, now: u64) -> bool {
        self.active_connections.load(Ordering::Acquire) == 0
            && now.saturating_sub(self.last_used_seconds.load(Ordering::Acquire))
                >= ROOM_TTL_SECONDS
    }
}

struct RoomConnection {
    room: Arc<Room>,
    state: AppState,
    active: bool,
}

impl RoomConnection {
    fn disconnect(&mut self) -> usize {
        if !self.active {
            return self.room.active_connections.load(Ordering::Acquire);
        }
        self.active = false;
        let count = self.room.active_connections.fetch_sub(1, Ordering::AcqRel) - 1;
        if count == 0 {
            self.room
                .last_used_seconds
                .store(self.state.elapsed_seconds(), Ordering::Release);
        }
        count
    }
}

impl Drop for RoomConnection {
    fn drop(&mut self) {
        let count = self.disconnect();
        publish_presence(&self.room.messages, count);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let state = AppState {
        rooms: Arc::new(Mutex::new(HashMap::new())),
        started_at: Instant::now(),
    };
    tokio::spawn(cleanup_expired_rooms(state.clone()));
    let app = Router::new()
        .route("/", get(chat_page))
        .route("/rooms", post(create_room))
        .route("/ws/{room_code}", get(websocket_handler))
        .route("/zpix.ttf", get(zpix_font))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("connect at http://{}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn chat_page() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn zpix_font() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "font/ttf")],
        Bytes::from_static(include_bytes!("../static/zpix.ttf")),
    )
}

async fn create_room(State(state): State<AppState>) -> Json<String> {
    let mut rooms = state.rooms.lock().await;
    let code = loop {
        let candidate = Alphanumeric.sample_string(&mut rand::rng(), ROOM_CODE_LENGTH);
        if !rooms.contains_key(&candidate) {
            break candidate;
        }
    };
    let (messages, _) = broadcast::channel(256);
    let room = Arc::new(Room {
        messages,
        active_connections: AtomicUsize::new(0),
        last_used_seconds: AtomicU64::new(state.elapsed_seconds()),
    });
    rooms.insert(code.clone(), room);
    Json(code)
}

async fn websocket_handler(
    State(state): State<AppState>,
    Path(room_code): Path<String>,
    websocket: WebSocketUpgrade,
) -> Response {
    let room = {
        let mut rooms = state.rooms.lock().await;
        match rooms.get(&room_code).cloned() {
            Some(room) if !room.is_expired(state.elapsed_seconds()) => {
                room.active_connections.fetch_add(1, Ordering::AcqRel);
                room.last_used_seconds
                    .store(state.elapsed_seconds(), Ordering::Release);
                Some(room)
            }
            Some(_) => {
                rooms.remove(&room_code);
                None
            }
            None => None,
        }
    };

    let Some(room) = room else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let connection = RoomConnection {
        room: Arc::clone(&room),
        state,
        active: true,
    };
    websocket
        .on_upgrade(move |socket| {
            handle_socket(
                socket,
                room.messages.clone(),
                room.messages.subscribe(),
                connection,
            )
        })
        .into_response()
}

async fn cleanup_expired_rooms(state: AppState) {
    let mut interval = tokio::time::interval(ROOM_CLEANUP_INTERVAL);
    loop {
        interval.tick().await;
        let now = state.elapsed_seconds();
        state
            .rooms
            .lock()
            .await
            .retain(|_, room| !room.is_expired(now));
    }
}

async fn handle_socket(
    socket: WebSocket,
    message_sender: broadcast::Sender<String>,
    mut messages: broadcast::Receiver<String>,
    connection: RoomConnection,
) {
    let (mut sender, mut receiver) = socket.split();
    publish_presence(
        &message_sender,
        connection.room.active_connections.load(Ordering::Acquire),
    );

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(message)
                                if !message.nickname.trim().is_empty()
                                    && message.nickname.len() <= 24
                                    && !message.text.is_empty()
                                    && message.text.len() <= 4096
                                    && message.client_id.len() <= 128 =>
                            {
                                let event = serde_json::json!({
                                    "type": "message",
                                    "clientId": message.client_id,
                                    "nickname": message.nickname,
                                    "text": message.text
                                });
                                let _ = message_sender.send(event.to_string());
                            }
                            Ok(_) => eprintln!("message has invalid fields"),
                            Err(error) => eprintln!("malformed chat message: {error}"),
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            outgoing = messages.recv() => {
                match outgoing {
                    Ok(text) => {
                        if sender.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

fn publish_presence(sender: &broadcast::Sender<String>, count: usize) {
    let event = serde_json::json!({
        "type": "presence",
        "count": count
    });
    let _ = sender.send(event.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_codes_have_the_required_length_and_charset() {
        let code = Alphanumeric.sample_string(&mut rand::rng(), ROOM_CODE_LENGTH);

        assert_eq!(code.len(), ROOM_CODE_LENGTH);
        assert!(code.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    }

    #[test]
    fn connected_rooms_do_not_expire() {
        let (messages, _) = broadcast::channel(1);
        let room = Room {
            messages,
            active_connections: AtomicUsize::new(1),
            last_used_seconds: AtomicU64::new(0),
        };

        assert!(!room.is_expired(ROOM_TTL_SECONDS));
    }

    #[test]
    fn disconnected_rooms_expire_after_24_hours() {
        let (messages, _) = broadcast::channel(1);
        let room = Room {
            messages,
            active_connections: AtomicUsize::new(0),
            last_used_seconds: AtomicU64::new(0),
        };

        assert!(!room.is_expired(ROOM_TTL_SECONDS - 1));
        assert!(room.is_expired(ROOM_TTL_SECONDS));
    }

    #[test]
    fn disconnect_broadcasts_the_updated_room_count() {
        let (messages, _) = broadcast::channel(4);
        let room = Arc::new(Room {
            messages,
            active_connections: AtomicUsize::new(1),
            last_used_seconds: AtomicU64::new(0),
        });
        let state = AppState {
            rooms: Arc::new(Mutex::new(HashMap::new())),
            started_at: Instant::now(),
        };
        let mut events = room.messages.subscribe();
        let connection = RoomConnection {
            room,
            state,
            active: true,
        };

        drop(connection);

        let event: serde_json::Value =
            serde_json::from_str(&events.try_recv().expect("presence event")).unwrap();
        assert_eq!(event["type"], "presence");
        assert_eq!(event["count"], 0);
    }
}
