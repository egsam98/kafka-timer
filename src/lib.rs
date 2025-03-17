use itertools::Itertools;
use parking_lot::Mutex;
use redis_module::{redis_module, alloc::RedisAlloc, Context, RedisError, RedisResult, RedisString, Status};
use timer::Handle;

mod timer;

static HANDLE: Mutex<Option<Handle>> = Mutex::new(None);

fn load(cx: &Context, args: &[RedisString]) -> RedisResult<()> {
    // Accepts librdkafka properties in `{key}={value}` format
    // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    let kafka_props = args.iter()
        .map(|arg| arg
            .to_string()
            .split_once('=')
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .ok_or(RedisError::WrongArity)
        )
        .try_collect()?;

    let handle = timer::serve(cx, kafka_props)?;
    *HANDLE.lock() = Some(handle);
    Ok(())
}

fn init(cx: &Context, args: &[RedisString]) -> Status {
    match load(cx, args) {
        Ok(()) => Status::Ok,
        Err(err) => {
            cx.log_warning(&err.to_string());
            Status::Err
        }
    }
}

fn deinit(_: &Context) -> Status {
    HANDLE.lock().take().unwrap().stop();
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
