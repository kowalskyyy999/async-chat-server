use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::{io, net::SocketAddr};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{
        Mutex,
        broadcast::{self, error::RecvError},
        mpsc,
    },
};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct User {
    id: Uuid,
    connected: bool,
    addr: Option<SocketAddr>, // active connection, None when offline
}

type Msg = (SocketAddr, String); // (sender, text)
type Users = Arc<Mutex<HashMap<String, User>>>;

enum Target<'a> {
    User(&'a str),
    Room(&'a str),
}

struct Parsed<'a> {
    target: Target<'a>,
    message: &'a str,
}

fn parse_message(line: &str) -> Option<Parsed<'_>> {
    let rest = line.strip_prefix("/from ")?;
    let (_from, rest) = rest.split_once(' ')?; // ignored, server uses the authenticated nickname
    let (target, message) = rest.split_once(" /message ")?;

    let target = if let Some(n) = target.strip_prefix("/to ") {
        Target::User(n.trim())
    } else if let Some(r) = target.strip_prefix("/room ") {
        Target::Room(r.trim())
    } else {
        return None;
    };

    Some(Parsed { target, message })
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("listening on {}", listener.local_addr()?);

    let (tx, _) = broadcast::channel::<Msg>(100);
    let users: Users = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        let tx = tx.clone();
        let users = users.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, addr, tx, users).await {
                eprintln!("[{addr}] error: {e}");
            }
        });
    }
}

async fn handle_client(
    mut socket: TcpStream,
    addr: SocketAddr,
    tx: broadcast::Sender<Msg>,
    users: Users,
) -> io::Result<()> {
    println!("[{addr}] connected");
    let (reader, mut writer) = socket.split();
    let mut lines = BufReader::new(reader).lines();

    let (chat_tx, mut chat_rx) = mpsc::unbounded_channel::<String>();

    let nickname = loop {
        let Some(line) = lines.next_line().await? else {
            return Ok(());
        };

        match handshake(&line, &addr, &users, &chat_tx).await {
            Ok((nick, id)) => {
                writer.write_all(format!("OK {id}\n").as_bytes()).await?;
                break nick;
            }
            Err(e) => writer.write_all(format!("ERR {e}\n").as_bytes()).await?,
        }
    };

    let mut rx = tx.subscribe();
    let _ = tx.send((addr, format!("* {nickname} joined")));

    loop {
        tokio::select! {
            line = lines.next_line() => match line? {
                Some(line) => {
                    let _ = tx.send((addr, format!("[{nickname}] {line}")));},
                None => break
            },
            msg = rx.recv() => match msg {
                Ok((from, text)) => {
                    if from != addr {
                        writer.write_all(text.as_bytes()).await?;
                        writer.write_all(b"\n").await?;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    writer.write_all(format!("* you missed {n} message\n").as_bytes()).await?;
                }
                Err(RecvError::Closed) => break
            },
        }
    }

    let _ = tx.send((addr, format!("* {nickname} left")));
    {
        let mut users = users.lock().await;
        if let Some(user) = users.get_mut(&nickname) {
            user.connected = false;
        }
        let config = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(&*users, config).unwrap();
        std::fs::write("server.bin", bytes).unwrap();
    }
    println!("[{addr}] disconnected");

    Ok(())
}

async fn handshake(
    line: &str,
    addr: &SocketAddr,
    users: &Users,
    dm_tx: &UnboundedSender<String>,
) -> Result<(String, Uuid), String> {
    let mut users = users.lock().await;
    let config = bincode::config::standard();

    if Path::new("server.bin").exists() {
        let bytes =
            std::fs::read("server.bin").map_err(|e| format!("failed to read server.bin: {e}"))?;
        let (saved_users, _): (HashMap<String, User>, usize) =
            bincode::serde::decode_from_slice(&bytes, config)
                .map_err(|e| format!("failed to decode server.bin: {e}"))?;
        *users = saved_users;
    }

    if let Some(nick) = line.strip_prefix("/register ") {
        let nick = nick.trim();

        if nick.is_empty() || nick.contains(' ') {
            return Err("invalid nickname".into());
        }

        if users.contains_key(nick) {
            return Err("nickname taken".into());
        }

        let id = Uuid::new_v4();
        users.insert(
            nick.to_string(),
            User {
                id,
                connected: true,
                addr: Some(*addr),
            },
        );

        let bytes = bincode::serde::encode_to_vec(&*users, config)
            .map_err(|e| format!("failed to encode users: {e}"))?;
        std::fs::write("server.bin", bytes)
            .map_err(|e| format!("failed to write server.bin: {e}"))?;

        Ok((nick.to_string(), id))
    } else if let Some(nick) = line.strip_prefix("/login ") {
        let nick = nick.trim();
        match users.get_mut(nick) {
            None => Err("unknown user".to_string()),
            Some(u) if u.connected => Err("already connected".to_string()),
            Some(u) => {
                u.connected = true;
                u.addr = Some(*addr);
                let id = u.id;
                Ok((nick.to_string(), id))
            }
        }
    } else {
        Err("expected /register or /login".to_string())
    }
}
