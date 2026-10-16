use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
use std::{fs, io::Write};
use tokio::io::{AsyncWriteExt, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
};
use uuid::Uuid;

type ServerLines = Lines<BufReader<OwnedReadHalf>>;

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Me {
    id: Uuid,
    nickname: String,
    conversations: Option<Vec<Conversations>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Conversations {
    subject: String,
    r#type: ConversationType,
    chats: Vec<Chat>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
enum ConversationType {
    Private,
    Room,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Chat {
    nickname: String,
    message: String,
}

enum State {
    Lobby,
    Private(String),
    Room(String),
}

fn prompt(state: &State, nickname: Option<&str>) {
    match state {
        State::Lobby => println!(
            "Hello {} you're in [Lobby] use '/chat <nickname>' or '/join <room>' or '/quit' to exit.",
            nickname.unwrap_or_default()
        ),
        State::Private(n) => println!("[Private: {n}] /quit to go back and lobby"),
        State::Room(r) => println!("[Room: {r}] /quit to go back and lobby"),
    }
}

async fn build_message(sender: &str, message: &str, state: &State) -> io::Result<String> {
    let msg = match state {
        State::Private(nick) => {
            format!("/from {sender} /to {nick} /message {message}")
        }
        State::Room(room) => {
            format!("/from {sender} /room {room} /message {message}")
        }
        _ => {
            return Err(io::Error::other("error"));
        }
    };

    Ok(msg)
}

async fn read_reply(server: &mut ServerLines) -> io::Result<Result<Uuid, String>> {
    let Some(line) = server.next_line().await? else {
        return Err(io::ErrorKind::UnexpectedEof.into());
    };

    match line.strip_prefix("OK ") {
        Some(id) => Ok(Ok(id.parse().map_err(io::Error::other)?)),
        None => Ok(Err(line.strip_prefix("ERR ").unwrap_or(&line).to_string())),
    }
}

async fn init(writer: &mut OwnedWriteHalf, server: &mut ServerLines) -> std::io::Result<String> {
    let config = bincode::config::standard();

    if Path::new("me.bin").exists() {
        let bytes = std::fs::read("me.bin")?;
        let (me, _): (Me, usize) =
            bincode::serde::decode_from_slice(&bytes, config).map_err(io::Error::other)?;
        writer
            .write_all(format!("/login {}\n", me.nickname).as_bytes())
            .await?;

        return match read_reply(server).await? {
            Ok(_) => Ok(me.nickname),
            Err(e) => Err(io::Error::other(format!("login rejected: {e}"))),
        };
    }

    loop {
        let mut input = String::new();
        print!("Please enter your nickname: ");
        std::io::stdout().flush()?;
        std::io::stdin().read_line(&mut input)?;
        let nick = input.trim().to_string();

        writer
            .write_all(format!("/register {nick}\n").as_bytes())
            .await?;

        match read_reply(server).await? {
            Ok(id) => {
                let me = Me {
                    id,
                    nickname: nick,
                    conversations: None,
                };

                let bytes = bincode::serde::encode_to_vec(&me, config).map_err(io::Error::other)?;
                std::fs::write("me.bin", bytes)?;
                return Ok(me.nickname);
            }
            Err(e) => {
                println!("Registration failed: {e}")
            }
        }
    }
}

#[derive(Debug)]
enum Target<'a> {
    Private(&'a str),
    Room(&'a str),
}

#[derive(Debug)]
struct Parsed<'a> {
    target: Target<'a>,
    message: &'a str,
}

fn parse_message(msg: &str) -> Option<Parsed<'_>> {
    let (_, rest) = msg.split_once("/from ")?;
    let (_from, rest) = rest.split_once(' ')?;
    let (_dest, message) = rest.split_once(" /message ")?;

    let target = if let Some(n) = _dest.strip_prefix("/to ") {
        Target::Private(_from.trim())
    } else if let Some(r) = _dest.strip_prefix("/room ") {
        Target::Room(r.trim())
    } else {
        return None;
    };

    Some(Parsed { target, message })
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into());
    let stream = TcpStream::connect(addr).await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut server = BufReader::new(reader).lines();

    let nickname = init(&mut writer, &mut server).await.unwrap();
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut state = State::Lobby;
    prompt(&state, Some(&nickname));
    let mut conversation = false;
    print!("What do you want: ");
    io::stdout().flush()?;
    loop {
        tokio::select! {
            msg = server.next_line() => match msg? {
                Some(msg) => {
                    if msg.contains("*") {
                        continue;
                    } else {
                        if let Some(res) = parse_message(&msg) {
                            let target = &res.target;
                            let message = &res.message;
                            match target {
                                Target::Private(n) => {
                                    println!("\n[Reply Private: {n}]: {message}")
                                },
                                Target::Room(r) => {
                                    println!("\n[Reply Room: {r}: {message}")
                                }
                            }
                        } else {
                            continue;
                        }
                    }
                },
                None => {
                    println!("server closed");
                }
            },
            line = stdin.next_line() => {
                let my_nickname = nickname.clone();

                let Some(line) = line? else { break };
                let line = line.trim();
                if line.is_empty() { continue; }

                if line.starts_with('/') {
                    if let Some(cmd) = line.strip_prefix('/') {
                        let (name, arg) = match cmd.split_once(' ') {
                            Some((n, a)) => (n, a.trim()),
                            None => (cmd, "")
                        };

                        match name {
                            "chat" if !arg.is_empty() && !arg.contains(' ') => {
                                if let State::Room(r) = &state {
                                    println!("Leave from room");
                                }
                                state = State::Private(arg.to_string());
                                prompt(&state, None);
                                conversation = false;
                            },
                            "rooms" => {
                                println!("Check rooms");
                            },
                            "join" if !arg.is_empty() && !arg.contains(' ') => {
                                if let State::Private(n) = &state {
                                    println!("Left the chat");
                                }
                                state = State::Room(arg.to_string());
                                prompt(&state, None);
                                conversation = false;

                            }
                            "quit" => {
                                if let State::Private(n) = &state {
                                    println!("Left the chat")
                                }

                                if let State::Room(r) = &state {
                                    println!("Leave the room")
                                }

                                if let State::Lobby = &state {
                                    println!("Closed");
                                    break
                                }
                                state = State::Lobby;
                                prompt(&state, Some(&my_nickname));
                            }
                            _ => {
                                println!("unknown or malformed command")
                            }
                        }
                    }
                }

                if conversation {
                    let message = build_message(&my_nickname, &line, &state).await.map_err(io::Error::other)?;
                    writer.write_all(message.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                }
                match state {
                    State::Lobby => {
                        print!("What do you want: ");
                        io::stdout().flush()?;
                        conversation = false;
                    },
                    State::Private(ref n) => {
                        print!("[Private: {n}]: ");
                        io::stdout().flush()?;
                        conversation = true;

                    },
                    State::Room(ref r) => {
                        print!("[Room: {r}]: ");
                        io::stdout().flush()?;
                        conversation = true;
                    },
                }


            },
            _ = tokio::signal::ctrl_c() => {
                println!("Terminated");
                break;
            }
        }
    }
    Ok(())
}
