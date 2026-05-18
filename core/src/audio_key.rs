use std::{collections::HashMap, io::Write, time::Duration};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::{Error, FileId, SpotifyId, packet::PacketType, util::SeqGenerator};

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub struct AudioKey(pub [u8; 16]);

#[derive(Debug, Error)]
pub enum AudioKeyError {
    #[error("audio key error")]
    AesKey,
    #[error("audio key rate limited by server")]
    RateLimited,
    #[error("other end of channel disconnected")]
    Channel,
    #[error("unexpected packet type {0}")]
    Packet(u8),
    #[error("sequence {0} not pending")]
    Sequence(u32),
    #[error("audio key response timeout")]
    Timeout,
}

impl From<AudioKeyError> for Error {
    fn from(err: AudioKeyError) -> Self {
        match err {
            AudioKeyError::AesKey => Error::unavailable(err),
            AudioKeyError::RateLimited => Error::resource_exhausted(err),
            AudioKeyError::Channel => Error::aborted(err),
            AudioKeyError::Sequence(_) => Error::aborted(err),
            AudioKeyError::Packet(_) => Error::unimplemented(err),
            AudioKeyError::Timeout => Error::aborted(err),
        }
    }
}

component! {
    AudioKeyManager : AudioKeyManagerInner {
        sequence: SeqGenerator<u32> = SeqGenerator::new(0),
        pending: HashMap<u32, oneshot::Sender<Result<AudioKey, AudioKeyError>>> = HashMap::new(),
    }
}

impl AudioKeyManager {
    pub(crate) fn dispatch(&self, cmd: PacketType, mut data: Bytes) -> Result<(), Error> {
        let seq = BigEndian::read_u32(data.split_to(4).as_ref());

        let sender = self
            .lock(|inner| inner.pending.remove(&seq))
            .ok_or(AudioKeyError::Sequence(seq))?;

        match cmd {
            PacketType::AesKey => {
                let mut key = [0u8; 16];
                key.copy_from_slice(data.as_ref());
                sender
                    .send(Ok(AudioKey(key)))
                    .map_err(|_| AudioKeyError::Channel)?
            }
            PacketType::AesKeyError => {
                let error_code = data.as_ref()[1];
                if error_code == 0x02 {
                    warn!("Audio key rate limited by server (error code 0x{:02x})", error_code);
                    sender
                        .send(Err(AudioKeyError::RateLimited))
                        .map_err(|_| AudioKeyError::Channel)?
                } else {
                    error!(
                        "error audio key {:x} {:x}",
                        data.as_ref()[0],
                        data.as_ref()[1]
                    );
                    sender
                        .send(Err(AudioKeyError::AesKey))
                        .map_err(|_| AudioKeyError::Channel)?
                }
            }
            _ => {
                trace!("Did not expect {cmd:?} AES key packet with data {data:#?}");
                return Err(AudioKeyError::Packet(cmd as u8).into());
            }
        }

        Ok(())
    }

    pub async fn request(&self, track: SpotifyId, file: FileId) -> Result<AudioKey, Error> {
        const MAX_RETRIES: u32 = 5;
        const BASE_BACKOFF_MS: u64 = 500;
        const KEY_RESPONSE_TIMEOUT: Duration = Duration::from_millis(10000);

        for attempt in 0..MAX_RETRIES {
            let (tx, rx) = oneshot::channel();

            let seq = self.lock(move |inner| {
                let seq = inner.sequence.get();
                inner.pending.insert(seq, tx);
                seq
            });

            self.send_key_request(seq, track, file)?;

            let result = match tokio::time::timeout(KEY_RESPONSE_TIMEOUT, rx).await {
                Err(_) => {
                    // Remove pending entry on timeout
                    self.lock(|inner| inner.pending.remove(&seq));
                    // Retry on timeout — can happen after session reconnect when the
                    // previous Shannon channel is gone but new one isn't dispatching yet.
                    let backoff_ms = BASE_BACKOFF_MS * (1u64 << attempt);
                    warn!(
                        "Audio key timeout, retry {}/{} after {}ms",
                        attempt + 1,
                        MAX_RETRIES,
                        backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    continue;
                }
                Ok(Ok(Ok(key))) => return Ok(key),
                Ok(Ok(Err(AudioKeyError::RateLimited))) => {
                    let backoff_ms = BASE_BACKOFF_MS * (1u64 << attempt);
                    // Add jitter: 0-25% of backoff
                    let jitter_ms = (backoff_ms / 4).max(1);
                    let total_ms = backoff_ms + (seq as u64 % jitter_ms);
                    warn!(
                        "Audio key rate limited, retry {}/{} after {}ms",
                        attempt + 1,
                        MAX_RETRIES,
                        total_ms
                    );
                    tokio::time::sleep(Duration::from_millis(total_ms)).await;
                    continue;
                }
                Ok(Ok(Err(e))) => Err(e.into()),
                Ok(Err(_)) => Err(AudioKeyError::Channel.into()),
            };

            return result;
        }

        error!("Audio key request failed after {} retries (timeout/rate limited)", MAX_RETRIES);
        Err(AudioKeyError::RateLimited.into())
    }

    fn send_key_request(&self, seq: u32, track: SpotifyId, file: FileId) -> Result<(), Error> {
        let mut data: Vec<u8> = Vec::new();
        data.write_all(&file.0)?;
        data.write_all(&track.to_raw())?;
        data.write_u32::<BigEndian>(seq)?;
        data.write_u16::<BigEndian>(0x0000)?;

        self.session().send_packet(PacketType::RequestKey, data)
    }
}
