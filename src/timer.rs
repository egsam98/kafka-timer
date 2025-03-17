use std::{collections::HashMap, thread::{self, JoinHandle}, time::{Duration, SystemTime, UNIX_EPOCH}};

use crossbeam::{channel::{self, Sender}, select};
use futures::{future, TryFutureExt};
use itertools::Itertools;
use rdkafka::{message::{Header, OwnedHeaders}, producer::{FutureProducer, FutureRecord}, ClientConfig, Message};
use redis_module::{key::HashSetFlags, Context, DetachedContext, NextArg, RedisError, RedisResult, RedisString, ZAddFlags, REDIS_OK};
use serde::Deserialize;

const KEY_MESSAGES: &str = "kafka-timer:messages";
const KEY_PQUEUE: &str = "kafka-timer:pqueue";
const RANGE_COUNT: usize = 1000;

/// KTIMER.ADD accepts arguments:
/// - Timeout in seconds to produce record to Kafka
/// - Kafka record in JSON format
/// Duplicate record keys are ignored.
pub fn add(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let timeout_secs = args.next_u64()?;
    let rec_raw = args.next_arg()?;
    let rec = serde_json::from_slice::<Record>(&rec_raw)?;

    let msg_key = ctx.create_string(KEY_MESSAGES);
    let hash_key = ctx.create_string(rec.key);
    let n = ctx.open_key_writable(&msg_key)
        .hash_set(&hash_key, &rec_raw, HashSetFlags::NX | HashSetFlags::COUNT_ALL)?;
    if n == 0 {
        return REDIS_OK;
    }

    let pqueue_key = ctx.create_string(KEY_PQUEUE);
    ctx.open_key_writable(&pqueue_key).zset_add(
        (unix_now() + timeout_secs) as f64, 
        &hash_key, 
        ZAddFlags::NX,
    )?;
    REDIS_OK
}

/// Runs background job handling added timers via [add] every second for every [RANGE_COUNT] keys
pub fn serve(cx: &Context, kafka_props: Vec<(String, String)>) -> RedisResult<Handle> {
    let producer = ClientConfig::from_iter(kafka_props).create::<FutureProducer>()?;

    let detached_cx = DetachedContext::new();
    detached_cx.set_context(cx)?;
    let (cancel_tx, cancel_rx) = channel::bounded::<()>(0);

    let handle = thread::spawn(move || {
        loop {
            let sleep = channel::after(Duration::from_secs(1));
            select! {
                recv(cancel_rx) -> _ => return,
                recv(sleep) -> _ => {},
            }

            loop {
                match handle(&detached_cx, &producer) {
                    Ok(true) => {},
                    Ok(false) => break,
                    Err(err) => {
                        detached_cx.log_warning(&err.to_string());
                        break;
                    },
                }
            }
        }
    });

    Ok(Handle { inner: handle, cancel_tx })
}

/// Returned by serve for graceful shutdown as [Self::stop] 
pub struct Handle {
    inner: JoinHandle<()>,
    cancel_tx: Sender<()>,
}

impl Handle {
    pub fn stop(self) {
        _ = self.cancel_tx.send(());
        self.inner.join().unwrap()
    }
}

#[derive(Deserialize, Debug)]
struct Record {
    topic: String,
    key: String,
    value: String,
    headers: Option<HashMap<String, serde_json::Value>>,
}

fn handle(cx: &DetachedContext, producer: &FutureProducer) -> RedisResult<bool> {
    let pqueue_key = cx.lock().create_string(KEY_PQUEUE);

    // Obtain record keys up to [RANGE_COUNT] from sorted set
    let cx_guard = cx.lock();
    let (record_keys, has_next) = match cx_guard
        .open_key(&pqueue_key)
        .zset_score_range(0_f64..=unix_now() as f64, false)
    {
        Ok(mut it) => (
            it.by_ref().take(RANGE_COUNT).collect_vec(),
            it.next().is_some(),
        ),
        Err(RedisError::WrongType) => return Ok(false),
        Err(err) => return Err(err),
    };
    drop(cx_guard);

    // Find corresponding records from hash map
    let msg_key = cx.lock().create_string(KEY_MESSAGES);
    let cx_guard = cx.lock();
    let Some(records) = cx_guard.open_key(&msg_key).hash_get_multi::<RedisString, RedisString>(&record_keys)? else {
        return Ok(has_next);
    };
    drop(cx_guard);
    let records: Vec<(RedisString, Record)> = records
        .into_iter()
        .map(|(key, rec_raw)| -> Result<(RedisString, Record), serde_json::Error> {
            let record = serde_json::from_slice::<Record>(&rec_raw)?;
            Ok((key, record))
        })
        .try_collect()?;

    // Produce found records into Kafka
    let produce_futures = records
        .iter()
        .map(|(key, rec)| {
            let mut record = FutureRecord::to(&rec.topic)
                .key(&rec.key)
                .payload(&rec.value);
            if let Some(headers) = &rec.headers {
                let fut_headers = headers
                    .iter()
                    .fold(OwnedHeaders::new(), |acc, (key, value)| {
                        acc.insert(Header { key, value: Some(&value.to_string()) })
                    });
                record = record.headers(fut_headers);
            }

            producer
                .send(record, Duration::from_secs(10))
                .map_ok(move |_| key)
        });

    // Await produce results and remove records from sorted set and hashmap
    let produced_records = futures::executor::block_on(future::join_all(produce_futures))
        .into_iter()
        .filter_map(|result| {
            result
                .inspect_err(|(err, msg)| {
                    cx.log_warning(&format!("Kafka error: {} (topic = {})", err, msg.topic()));
                })
                .ok()
        })
        .map(|key| -> RedisResult<()> {
            let cx_guard = cx.lock();
            cx_guard.open_key_writable(&pqueue_key).zset_rem(key)?;
            cx_guard.open_key_writable(&msg_key).hash_del(key)?;
            Ok(())
        })
        .fold_ok(0_usize, |acc, _| acc + 1)?;

    if produced_records > 0 {
        cx.log_notice(&format!("Produced {} records", produced_records));
    }
    Ok(has_next)
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
