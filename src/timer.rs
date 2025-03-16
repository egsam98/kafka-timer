use std::{collections::HashMap, thread::{self, JoinHandle}, time::{Duration, SystemTime, UNIX_EPOCH}};

use crossbeam::{channel::{self, Receiver}, select};
use futures::{future, TryFutureExt};
use itertools::Itertools;
use rdkafka::{error::KafkaResult, message::{Header, OwnedHeaders}, producer::{FutureProducer, FutureRecord}, ClientConfig};
use redis_module::{key::HashSetFlags, Context, DetachedContext, NextArg, RedisError, RedisResult, RedisString, ZAddFlags, REDIS_OK};
use serde::Deserialize;

const KEY_MESSAGES: &str = "kafka-timer:messages";
const KEY_TIMERS: &str = "kafka-timer:pqueue";
const RANGE_COUNT: usize = 1000;

#[derive(Deserialize, Debug)]
struct Record {
    topic: String,
    key: String,
    value: String,
    headers: Option<HashMap<String, serde_json::Value>>,
}

pub fn add(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let delay_secs = args.next_u64()?;
    let rec_raw = args.next_arg()?;
    let rec = serde_json::from_slice::<Record>(&rec_raw)?;

    let msg_key = ctx.create_string(KEY_MESSAGES);
    let hash_key = ctx.create_string(rec.key);
    let n = ctx.open_key_writable(&msg_key)
        .hash_set(&hash_key, &rec_raw, HashSetFlags::NX | HashSetFlags::COUNT_ALL)?;
    if n == 0 {
        return REDIS_OK;
    }

    let zset_key = ctx.create_string(KEY_TIMERS);
    ctx.open_key_writable(&zset_key).zset_add(
        (unix_now() + delay_secs) as f64, 
        &hash_key, 
        ZAddFlags::NX,
    )?;
    REDIS_OK
}

pub fn serve(cx: &Context, cancel: Receiver<()>, kafka_opts: Vec<(String, String)>) -> KafkaResult<JoinHandle<()>> {
    let producer = ClientConfig::from_iter(kafka_opts).create::<FutureProducer>()?;

    let detached_cx = DetachedContext::new();
    detached_cx.set_context(cx).unwrap();

    let handle = thread::spawn(move || {
        loop {
            let sleep = channel::after(Duration::from_secs(1));
            select! {
                recv(cancel) -> _ => return,
                recv(sleep) -> _ => {},
            }
    
            loop {
                match handle(&detached_cx, &producer) {
                    Ok(n) if n == 0 => break,
                    Err(err) => {
                        detached_cx.log_warning(&err.to_string());
                        break;
                    },
                    _ => {},
                }
            }
        }
    });
    Ok(handle)
}

pub fn handle(cx: &DetachedContext, producer: &FutureProducer) -> RedisResult<usize> {
    let zset_key = cx.lock().create_string(KEY_TIMERS);
    let record_keys = match cx
        .lock()
        .open_key(&zset_key)
        .zset_score_range(0_f64..=unix_now() as f64, false)
    {
        Ok(it) => it.take(RANGE_COUNT).collect_vec(),
        Err(RedisError::WrongType) => return Ok(0),
        Err(err) => return Err(err) 
    };

    let msg_key = cx.lock().create_string(KEY_MESSAGES);
    let Some(records) = cx.lock().open_key(&msg_key).hash_get_multi::<RedisString, RedisString>(&record_keys)? else {
        return Ok(0);
    };
    let records: Vec<(RedisString, Record)> = records
        .into_iter()
        .map(|(key, rec_raw)| -> Result<(RedisString, Record), serde_json::Error> {
            let record = serde_json::from_slice::<Record>(&rec_raw)?;
            Ok((key, record))
        })
        .try_collect()?;
    if records.is_empty() {
        return Ok(0);
    }

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

    let produced_records = futures::executor::block_on(future::join_all(produce_futures))
        .into_iter()
        .filter_map(|result| match result {
            Ok(key) => Some(key),
            Err((err, record)) => {
                cx.log_warning(&format!("Kafka error: {} (record = {:?})", err, record));
                None
            }
        })
        .map(|key| -> RedisResult<()> {
            let cx_guard = cx.lock();
            cx_guard.open_key_writable(&zset_key).zset_rem(key)?;
            cx_guard.open_key_writable(&msg_key).hash_del(key)?;
            Ok(())
        })
        .fold_ok(0_usize, |acc, _| acc + 1)?;

    cx.log_notice(&format!("Produced {} records", produced_records));
    Ok(record_keys.len())
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}
