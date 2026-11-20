use anyhow::{Context, Result, anyhow};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::process::Command;

use crate::protocol::action::Action;
use crate::session::Link;
use crate::transport;

pub struct AutoConfig {
    pub call_eq: String,
    pub call_bass: String,
    pub call_anc: String,
    pub normal_eq: String,
    pub normal_bass: String,
    pub normal_anc: String,
    pub notify: bool,
}

pub async fn notify(summary: &str, body: &str) {
    let _ =
        Command::new("notify-send").args(["-a", "nothingctl", "-i", "audio-headphones", summary, body]).status().await;
}

pub async fn has_active_recording() -> Result<bool> {
    let output = Command::new("pactl")
        .args(["list", "source-outputs", "short"])
        .output()
        .await
        .context("failed to run pactl list source-outputs")?;
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(!text.trim().is_empty())
}

async fn apply_profile<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    link: &mut Link<R, W>,
    eq: &str,
    bass: &str,
    anc: &str,
) -> Result<()> {
    let eq_action = Action::parse(&["eq", eq]).map_err(|e| anyhow!(e))?;
    let bass_action = Action::parse(&["bass", bass]).map_err(|e| anyhow!(e))?;
    let anc_action = Action::parse(&["anc", anc]).map_err(|e| anyhow!(e))?;

    link.apply(&eq_action).await.map_err(|e| anyhow!(e))?;
    link.apply(&bass_action).await.map_err(|e| anyhow!(e))?;
    link.apply(&anc_action).await.map_err(|e| anyhow!(e))?;
    Ok(())
}

pub async fn run_auto(device: Option<&str>, config: AutoConfig) -> Result<()> {
    loop {
        match run_session(device, &config).await {
            Ok(()) => break,
            Err(e) => {
                crate::debug!("auto session error: {e:#}");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
    Ok(())
}

async fn run_session(device: Option<&str>, config: &AutoConfig) -> Result<()> {
    let conn = transport::open(device).await?;
    let (r, w) = tokio::io::split(conn.stream);
    let mut link = Link::new(conn.model, r, w);
    link.connect_sequence().await.context("failed initial connect sequence")?;

    let mut in_call = has_active_recording().await.unwrap_or(false);
    if in_call {
        apply_profile(&mut link, &config.call_eq, &config.call_bass, &config.call_anc).await?;
        if config.notify {
            notify("Call mode active", "Switched to voice profile").await;
        }
    } else {
        apply_profile(&mut link, &config.normal_eq, &config.normal_bass, &config.normal_anc).await?;
    }

    let mut pactl_child = Command::new("pactl")
        .arg("subscribe")
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to spawn pactl subscribe")?;

    let stdout = pactl_child.stdout.take().ok_or_else(|| anyhow!("missing pactl stdout"))?;
    let mut lines = BufReader::new(stdout).lines();

    loop {
        tokio::select! {
            line_result = lines.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        if line.contains("source-output") {
                            let recording = has_active_recording().await.unwrap_or(false);
                            if recording != in_call {
                                in_call = recording;
                                if in_call {
                                    apply_profile(&mut link, &config.call_eq, &config.call_bass, &config.call_anc).await?;
                                    if config.notify {
                                        notify("Call detected", &format!("EQ: {}, Bass: {}, ANC: {}", config.call_eq, config.call_bass, config.call_anc)).await;
                                    }
                                } else {
                                    apply_profile(&mut link, &config.normal_eq, &config.normal_bass, &config.normal_anc).await?;
                                    if config.notify {
                                        notify("Call ended", &format!("EQ: {}, Bass: {}, ANC: {}", config.normal_eq, config.normal_bass, config.normal_anc)).await;
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        crate::debug!("pactl reader error: {e}");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(10)) => {
                let recording = has_active_recording().await.unwrap_or(false);
                if recording != in_call {
                    in_call = recording;
                    if in_call {
                        apply_profile(&mut link, &config.call_eq, &config.call_bass, &config.call_anc).await?;
                        if config.notify {
                            notify("Call detected", &format!("EQ: {}, Bass: {}, ANC: {}", config.call_eq, config.call_bass, config.call_anc)).await;
                        }
                    } else {
                        apply_profile(&mut link, &config.normal_eq, &config.normal_bass, &config.normal_anc).await?;
                        if config.notify {
                            notify("Call ended", &format!("EQ: {}, Bass: {}, ANC: {}", config.normal_eq, config.normal_bass, config.normal_anc)).await;
                        }
                    }
                }
            }
        }
    }

    let _ = pactl_child.kill().await;
    Ok(())
}
