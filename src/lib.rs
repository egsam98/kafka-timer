use std::{sync::OnceLock, thread::JoinHandle};

use crossbeam::channel::{self, Sender};
use itertools::Itertools;
use parking_lot::Mutex;
use redis_module::{redis_module, alloc::RedisAlloc, Context, RedisError, RedisResult, RedisString, Status};

mod timer;

static HANDLE: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);
static CANCEL_TX: OnceLock<Sender<()>> = OnceLock::new();

fn load(cx: &Context, args: &[RedisString]) -> RedisResult<()> {
    let (tx, rx) = channel::bounded::<()>(0);
    let kafka_opts = args.iter()
        .map(|arg| arg
            .to_string()
            .split_once('=')
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .ok_or(RedisError::WrongArity)
        )
        .try_collect()?;

    let handle = timer::serve(cx, rx, kafka_opts)?;

    _ = CANCEL_TX.set(tx);
    *HANDLE.lock() = Some(handle);
    Ok(())
}

fn init(cx: &Context, args: &[RedisString]) -> Status {
    match load(cx, args) {
        Ok(_) => Status::Ok,
        Err(err) => {
            cx.log_warning(&err.to_string());
            Status::Err
        }
    }
}

fn deinit(_: &Context) -> Status {
    _ = CANCEL_TX.get().unwrap().send(());
    _ = HANDLE.lock().take().unwrap().join();
    Status::Ok
}

redis_module! {
    name: "kafka-timer",
    version: 1,
    allocator: (RedisAlloc, RedisAlloc),
    data_types: [],
    init: init,
    deinit: deinit,
    commands: [
        ["KTIMER.ADD", timer::add, "write fast deny-oom", 1, 2, 1, ""],
    ],
}
