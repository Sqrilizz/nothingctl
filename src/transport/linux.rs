use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bluer::rfcomm::{Profile, ProfileHandle, ReqError, Role, Stream};
use bluer::{Address, Device, Session, Uuid};
use futures::StreamExt;

use super::{Connection, DeviceInfo, UnsupportedModel, read_model_id, select};
use crate::protocol::model::{self, CONTROL_UUID, FAST_PAIR_UUID};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

fn uuid(s: &str) -> Uuid {
    s.parse().expect("constant uuid")
}

async fn info(device: &Device) -> bluer::Result<DeviceInfo> {
    let name = match device.alias().await {
        Ok(a) => a,
        Err(_) => device.name().await?.unwrap_or_default(),
    };
    Ok(DeviceInfo { address: device.address().to_string(), name, connected: device.is_connected().await? })
}

async fn matching(session: &Session) -> anyhow::Result<Vec<(Device, DeviceInfo)>> {
    let adapter = session.default_adapter().await.context("no Bluetooth adapter")?;
    let control = uuid(CONTROL_UUID);
    let mut out = Vec::new();
    for addr in adapter.device_addresses().await? {
        let device = adapter.device(addr)?;
        if !device.is_paired().await.unwrap_or(false) {
            continue;
        }
        let has_control = device.uuids().await.ok().flatten().is_some_and(|u| u.contains(&control));
        if has_control {
            let i = info(&device).await?;
            out.push((device, i));
        }
    }
    Ok(out)
}

pub async fn list() -> anyhow::Result<Vec<DeviceInfo>> {
    let session = Session::new().await.context("cannot reach BlueZ over D-Bus")?;
    Ok(matching(&session).await?.into_iter().map(|(_, i)| i).collect())
}

async fn connect_service(session: &Session, device: &Device, service: Uuid) -> anyhow::Result<(Stream, ProfileHandle)> {
    let mut handle = session
        .register_profile(Profile {
            uuid: service,
            role: Some(Role::Client),
            auto_connect: Some(false),
            ..Default::default()
        })
        .await
        .with_context(|| format!("BlueZ refused a client profile for {service}"))?;
    let addr: Address = device.address();
    let connect = device.connect_profile(&service);
    tokio::pin!(connect);
    let mut connect_done = false;
    let deadline = tokio::time::sleep(CONNECT_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            res = &mut connect, if !connect_done => {
                connect_done = true;
                if let Err(e) = res {
                    bail!("BlueZ could not connect service {service} on {addr} ({e}); nothing was sent");
                }
            }
            req = handle.next() => {
                let req = req.ok_or_else(|| anyhow!("BlueZ dropped the profile for {service}"))?;
                if req.device() == addr {
                    let stream = req.accept().context("accepting the RFCOMM socket")?;
                    return Ok((stream, handle));
                }
                req.reject(ReqError::Rejected);
            }
            _ = &mut deadline => bail!("timed out connecting service {service} on {addr}; nothing was sent"),
        }
    }
}

async fn fast_pair_model_id(session: &Session, device: &Device) -> anyhow::Result<String> {
    let (stream, handle) = connect_service(session, device, uuid(FAST_PAIR_UUID)).await?;
    let (reader, _unused_writer) = tokio::io::split(stream);
    let id = read_model_id(reader).await?;
    drop(handle);
    Ok(id)
}

pub async fn open(address: Option<&str>) -> anyhow::Result<Connection> {
    let session = Session::new().await.context("cannot reach BlueZ over D-Bus")?;
    let devices = matching(&session).await?;
    let chosen = select(devices.iter().map(|(_, i)| i.clone()).collect(), address)?;
    let device = devices
        .into_iter()
        .find(|(_, i)| i.address == chosen.address)
        .map(|(d, _)| d)
        .ok_or_else(|| anyhow!("{} disappeared", chosen.address))?;

    let (model, model_id) = match fast_pair_model_id(&session, &device).await {
        Ok(id) => {
            crate::debug!("model id {id}");
            let Some(m) = model::by_model_id(&id) else {
                return Err(UnsupportedModel { model_id: Some(id), address: chosen.address }.into());
            };
            (m, id)
        }
        Err(e) => {
            crate::debug!("fast pair read failed: {e}; falling back to device name matching");
            let Some(m) = model::MODELS.iter().copied().find(|m| chosen.name.eq_ignore_ascii_case(m.name)) else {
                return Err(e);
            };
            let id = m.model_ids.first().copied().unwrap_or("UNKNOWN").to_string();
            (m, id)
        }
    };

    let (stream, handle) = connect_service(&session, &device, uuid(CONTROL_UUID)).await?;
    Ok(Connection { device: chosen, model, model_id, stream: Box::new(stream), _guard: Box::new((handle, session)) })
}
