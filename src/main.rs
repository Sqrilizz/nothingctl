#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

mod auto;
mod log;
mod protocol;
mod session;
mod transport;

use std::process::ExitCode;

use anyhow::{Context, anyhow};
use clap::{Parser, Subcommand};
use tokio::io::AsyncBufReadExt;

use protocol::action::{Action, GRAMMAR};
use protocol::frame::OpCounter;
use protocol::model::{self, Model};
use protocol::state::{State, snapshot};
use session::{Link, planned_action, planned_connect};
use transport::UnsupportedModel;

const EXIT_UNSUPPORTED: u8 = 3;

#[derive(Parser)]
#[command(name = "nothingctl", version, about = "Control Nothing and CMF audio devices over Bluetooth")]
struct Cli {
    #[arg(long, global = true)]
    dry_run: bool,
    #[arg(long, global = true, default_value = "B175")]
    model: String,
    #[arg(long, global = true)]
    firmware: Option<String>,
    #[arg(short, long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    List {
        #[arg(long)]
        json: bool,
    },
    Status {
        #[arg(short, long)]
        device: Option<String>,
        #[arg(long)]
        json: bool,
    },
    #[command(after_help = format!("Settings: {GRAMMAR}\nAvailable values depend on the model; `status` lists them."))]
    Set {
        #[arg(short, long)]
        device: Option<String>,
        #[arg(required = true, num_args = 1.., trailing_var_arg = true, allow_hyphen_values = true)]
        setting: Vec<String>,
    },
    Anc {
        #[arg(short, long)]
        device: Option<String>,
        #[arg(value_name = "MODE")]
        mode: Option<String>,
        #[arg(long)]
        notify: bool,
    },
    Auto {
        #[arg(short, long)]
        device: Option<String>,
        #[arg(long, default_value = "vocals")]
        call_eq: String,
        #[arg(long, default_value = "1")]
        call_bass: String,
        #[arg(long, default_value = "transparency")]
        call_anc: String,
        #[arg(long, default_value = "dirac")]
        normal_eq: String,
        #[arg(long, default_value = "3")]
        normal_bass: String,
        #[arg(long, default_value = "high")]
        normal_anc: String,
        #[arg(long)]
        notify: bool,
    },
    Watch {
        #[arg(short, long)]
        device: Option<String>,
    },
}

fn print_json(v: &impl serde::Serialize) {
    match serde_json::to_string(v) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("error: {e}"),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    log::set_verbose(cli.verbose);
    let result = if cli.dry_run { dry_run(&cli).await } else { run(&cli).await };
    match result {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            if e.downcast_ref::<UnsupportedModel>().is_some() {
                ExitCode::from(EXIT_UNSUPPORTED)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}

fn unsupported_line(e: &UnsupportedModel) -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "code": "unsupported-model",
        "modelId": e.model_id,
        "message": e.to_string(),
    })
}

fn print_frames(frames: &[(String, &str)]) {
    for (hex, label) in frames {
        println!("{hex}  {label}");
    }
}

async fn dry_run(cli: &Cli) -> anyhow::Result<ExitCode> {
    let model: &Model = model::by_base(&cli.model).ok_or_else(|| anyhow!("no table for model {}", cli.model))?;
    eprintln!("dry run for {} {}: nothing is sent", model.base, model.name);
    let mut ops = OpCounter::default();
    let state = State { firmware: cli.firmware.clone(), ..State::default() };
    match &cli.command {
        Cmd::List { .. } => anyhow::bail!("list sends nothing; run it without --dry-run"),
        Cmd::Status { .. } => print_frames(&planned_connect(model, cli.firmware.as_deref(), &mut ops)),
        Cmd::Set { setting, .. } => {
            let action = Action::parse(setting).map_err(|e| anyhow!(e))?;
            print_frames(&planned_action(model, &state, &action, &mut ops).map_err(|e| anyhow!(e))?);
        }
        Cmd::Anc { mode, .. } => {
            let target = mode.as_deref().unwrap_or("high");
            let action = Action::parse(&["anc", target]).map_err(|e| anyhow!(e))?;
            print_frames(&planned_action(model, &state, &action, &mut ops).map_err(|e| anyhow!(e))?);
        }
        Cmd::Auto { .. } => anyhow::bail!("auto mode requires real device connection"),
        Cmd::Watch { .. } => {
            print_frames(&planned_connect(model, cli.firmware.as_deref(), &mut ops));
            let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Some(line) = lines.next_line().await? {
                let args: Vec<&str> = line.split_whitespace().collect();
                if args.is_empty() {
                    continue;
                }
                match Action::parse(&args).and_then(|a| planned_action(model, &state, &a, &mut ops)) {
                    Ok(frames) => print_frames(&frames),
                    Err(message) => print_json(&serde_json::json!({"type": "error", "message": message})),
                }
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn run(cli: &Cli) -> anyhow::Result<ExitCode> {
    match &cli.command {
        Cmd::List { json } => {
            let devices = transport::list().await?;
            if *json {
                print_json(&devices);
            } else {
                for d in devices {
                    println!(
                        "{}  {}  {}",
                        d.address,
                        if d.connected { "connected   " } else { "disconnected" },
                        d.name
                    );
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Status { device, json } => {
            let conn = transport::open(device.as_deref()).await?;
            let (r, w) = tokio::io::split(conn.stream);
            let mut link = Link::new(conn.model, r, w);
            link.connect_sequence().await.context("link dropped during the status read")?;
            let snap = snapshot(link.model, &conn.model_id, &link.state, &conn.device.address, &conn.device.name);
            if *json {
                print_json(&snap);
            } else {
                print_status(&snap);
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Set { device, setting } => {
            let action = Action::parse(setting).map_err(|e| anyhow!(e))?;
            let conn = transport::open(device.as_deref()).await?;
            let (r, w) = tokio::io::split(conn.stream);
            let mut link = Link::new(conn.model, r, w);
            if action.needs_state(conn.model) {
                link.connect_sequence().await.context("link dropped during the status read")?;
            } else {
                action.frames(conn.model, &link.state).map_err(|e| anyhow!(e))?;
            }
            link.one_shot(&action).await.map_err(|e| anyhow!(e))?;
            print_json(&serde_json::json!({"type": "ack", "cmd": action.describe()}));
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Anc { device, mode, notify } => {
            let conn = transport::open(device.as_deref()).await?;
            let (r, w) = tokio::io::split(conn.stream);
            let mut link = Link::new(conn.model, r, w);
            link.connect_sequence().await.context("link dropped during the status read")?;

            let current_anc = link.state.anc_code.and_then(|c| conn.model.anc_by_code(c)).map(|m| m.name);

            match mode.as_deref() {
                None => {
                    let modes: Vec<&str> =
                        conn.model.anc(link.state.firmware.as_deref()).iter().map(|m| m.name).collect();
                    println!("current: {}", current_anc.unwrap_or("unknown"));
                    println!("available: {}", modes.join(", "));
                    Ok(ExitCode::SUCCESS)
                }
                Some("cycle") | Some("next") => {
                    let next_mode = match current_anc {
                        Some("high") | Some("mid") | Some("low") | Some("adaptive") => "transparency",
                        Some("transparency") => "off",
                        _ => "high",
                    };
                    let action = Action::parse(&["anc", next_mode]).map_err(|e| anyhow!(e))?;
                    link.one_shot(&action).await.map_err(|e| anyhow!(e))?;
                    if *notify {
                        auto::notify("Noise Control", next_mode).await;
                    }
                    print_json(
                        &serde_json::json!({"type": "ack", "cmd": format!("anc {next_mode}"), "mode": next_mode}),
                    );
                    Ok(ExitCode::SUCCESS)
                }
                Some(target) => {
                    let action = Action::parse(&["anc", target]).map_err(|e| anyhow!(e))?;
                    link.one_shot(&action).await.map_err(|e| anyhow!(e))?;
                    if *notify {
                        auto::notify("Noise Control", target).await;
                    }
                    print_json(&serde_json::json!({"type": "ack", "cmd": format!("anc {target}"), "mode": target}));
                    Ok(ExitCode::SUCCESS)
                }
            }
        }
        Cmd::Auto { device, call_eq, call_bass, call_anc, normal_eq, normal_bass, normal_anc, notify } => {
            let config = auto::AutoConfig {
                call_eq: call_eq.clone(),
                call_bass: call_bass.clone(),
                call_anc: call_anc.clone(),
                normal_eq: normal_eq.clone(),
                normal_bass: normal_bass.clone(),
                normal_anc: normal_anc.clone(),
                notify: *notify,
            };
            auto::run_auto(device.as_deref(), config).await?;
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Watch { device } => {
            let conn = match transport::open(device.as_deref()).await {
                Ok(conn) => conn,
                Err(e) => {
                    if let Some(u) = e.downcast_ref::<UnsupportedModel>() {
                        print_json(&unsupported_line(u));
                    }
                    return Err(e);
                }
            };
            let (r, w) = tokio::io::split(conn.stream);
            let mut link = Link::new(conn.model, r, w);
            link.model_id = conn.model_id.clone();
            let lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            let result = match link.connect_sequence().await {
                Ok(()) => link.watch(&conn.device.address, &conn.device.name, lines, &mut |v| print_json(&v)).await,
                Err(e) => Err(e),
            };
            match result {
                Ok(()) => Ok(ExitCode::SUCCESS),
                Err(e) => {
                    crate::debug!("link: {e}");
                    print_json(&serde_json::json!({"type": "disconnected"}));
                    Ok(ExitCode::FAILURE)
                }
            }
        }
    }
}

fn print_status(s: &protocol::state::Snapshot) {
    let or_unknown = |v: Option<&str>| v.unwrap_or("unknown").to_string();
    println!("{} ({})  {} {}", s.name, s.address, s.model.base, s.model.name);
    println!("firmware     {}", or_unknown(s.firmware.as_deref()));
    for b in &s.battery {
        println!("battery      {} {}%{}", b.id, b.level, if b.charging { " charging" } else { "" });
    }
    if let Some(anc) = &s.anc {
        println!("anc          {}  ({})", or_unknown(anc.mode), anc.available.join(", "));
    }
    println!("eq           {}  ({})", or_unknown(s.eq.preset), s.eq.available.join(", "));
    if let Some([bass, mid, treble]) = s.eq.custom {
        println!("custom eq    bass {bass} mid {mid} treble {treble}");
    }
    if let Some(sp) = &s.spatial {
        println!("spatial      {}  ({})", or_unknown(sp.mode), sp.available.join(", "));
    }
    let on_off = |on: bool| if on { "on" } else { "off" };
    if let Some(b) = &s.bass {
        let level = b.level.map_or("unknown".to_string(), |l| l.to_string());
        let enabled = or_unknown(b.enabled.map(on_off));
        println!("bass         level {level} of {}..{}, {enabled}", b.levels[0], b.levels[1]);
    }
    for t in model::TOGGLE_KEYS {
        if let Some(on) = s.settings.get(t.key).and_then(|v| v.as_bool()) {
            println!("{:<12} {}", t.name, on_off(on));
        }
    }
    for c in model::CHOICES {
        if let Some(v) = s.settings.get(c.key).filter(|v| !v.is_null()) {
            let available: Vec<&str> = c.modes.iter().map(|m| m.name).collect();
            println!("{:<12} {}  ({})", c.name, or_unknown(v["mode"].as_str()), available.join(", "));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_model_line() {
        let e = UnsupportedModel { model_id: Some("ABCDEF".into()), address: "AA:BB".into() };
        let line = serde_json::to_string(&unsupported_line(&e)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v.as_object().unwrap().len(), 4);
        assert_eq!(v["type"], "error");
        assert_eq!(v["code"], "unsupported-model");
        assert_eq!(v["modelId"], "ABCDEF");
        let message = v["message"].as_str().unwrap();
        assert!(message.contains("ABCDEF") && message.contains("B175") && message.contains("nothing was sent"));
        assert!(!line.contains('\n'));

        let none = unsupported_line(&UnsupportedModel { model_id: None, address: "AA:BB".into() });
        assert_eq!(none["code"], "unsupported-model");
        assert!(none["modelId"].is_null());
    }

    #[test]
    fn unsupported_exit_code_differs_from_link_failure() {
        let e: anyhow::Error = UnsupportedModel { model_id: None, address: "AA".into() }.into();
        assert!(e.downcast_ref::<UnsupportedModel>().is_some());
        assert_ne!(EXIT_UNSUPPORTED, 1);
        assert_ne!(EXIT_UNSUPPORTED, 2);
    }
}
