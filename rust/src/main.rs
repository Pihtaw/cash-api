use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use scc::hash_index::Entry;
use scc::HashIndex;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

type Cache = HashIndex<String, Box<[u8]>>;
struct Message {
    key: String,
    value: Option<Box<[u8]>>,
}

fn msg(key: &str, value: Option<&str>) -> Message {
    Message {
        key: key.to_string(),
        value: value.map(|v| v.as_bytes().into()),
    }
}

fn apply(cache: &Cache, m: Message) -> &'static str {
    match m.value {
        None => {
            if cache.remove_sync(m.key.as_str()) {
                "удалила"
            } else {
                "удалять нечего"
            }
        }
        Some(value) => match cache.entry_sync(m.key) {
            Entry::Occupied(mut old) => {
                old.update(value);
                "заменила"
            }
            Entry::Vacant(empty) => {
                empty.insert_entry(value);
                "вставила"
            }
        },
    }
}

// чтение
fn get(cache: &Cache, key: &str) -> Option<Vec<u8>> {
    cache.peek_with(key, |_key, value| value.to_vec())
}

// прогрев
fn warm_up(
    cache: &Cache,
    partitions: Vec<Vec<Message>>,
    ready: &AtomicBool,
    print: bool,
) -> Duration {
    let started = Instant::now();
    let high: Vec<usize> = partitions.iter().map(Vec::len).collect();
    let mut readers: Vec<_> = partitions
        .into_iter()
        .map(|p| p.into_iter().enumerate())
        .collect();
    let mut position = vec![0; high.len()];

    while position.iter().zip(&high).any(|(pos, h)| pos < h) {
        for (p, part) in readers.iter_mut().enumerate() {
            if let Some((offset, m)) = part.next() {
                let key = m.key.clone();
                let what = apply(cache, m);
                if print {
                    println!("  партиция {p}, offset {offset}: {key} -> {what}");
                }
                position[p] = offset + 1;
            }
        }
    }
    let took = started.elapsed();
    ready.store(true, Ordering::Release);
    took
}
fn http_get(cache: &Cache, ready: &AtomicBool, key: &str) -> String {
    if !ready.load(Ordering::Acquire) {
        return "503 cache is warming up".into();
    }
    match get(cache, key) {
        Some(v) => format!("200 {}", String::from_utf8_lossy(&v)),
        None => "404 not found".into(),
    }
}

fn fake_kafka() -> Vec<Vec<Message>> {
    vec![
        vec![
            msg("p-1", Some(r#"{"name":"Сыктывкар-1","version":1}"#)),
            msg(
                "p-1",
                Some(r#"{"name":"Сыктывкар-1 (переезд)","version":2}"#),
            ),
        ],
        vec![
            msg("p-2", Some(r#"{"name":"Москва-7","version":1}"#)),
            msg("p-2", None), // tombstone: пиццерию закрыли
        ],
        vec![msg("p-3", Some(r#"{"name":"Казань-2","version":1}"#))],
    ]
}

fn demo() {
    let cache = Cache::new();
    let ready = AtomicBool::new(false);
    println!(
        "до прогрева: GET p-1 -> {}",
        http_get(&cache, &ready, "p-1")
    );
    println!("== прогрев: читаю топик с начала");
    let took = warm_up(&cache, fake_kafka(), &ready, true);
    println!("== прогрев закончен за {took:?}");
    for key in ["p-1", "p-2", "p-3", "p-404"] {
        println!("  GET {key} -> {}", http_get(&cache, &ready, key));
    }
    println!(
        "новое сообщение после старта: p-3 -> {}",
        apply(
            &cache,
            msg("p-3", Some(r#"{"name":"Казань-2 (ремонт)","version":2}"#))
        )
    );
    println!("  GET p-3 -> {}", http_get(&cache, &ready, "p-3"));
}

fn make_value(id: usize, version: u64) -> Box<[u8]> {
    format!(r#"{{"id":"p-{id}","name":"Пиццерия номер {id}","city":"Сыктывкар","address":"ул. Ленина, {id}","open":true,"version":{version}}}"#)
        .into_bytes()
        .into_boxed_slice()
}

fn rss_mb() -> f64 {
    memory_stats::memory_stats()
        .map(|s| s.physical_mem as f64 / 1048576.0)
        .unwrap_or(0.0)
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn run_step(cache: &Arc<Cache>, n: usize, readers: usize, secs: f64) -> (u64, Vec<u64>, u64) {
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(readers + 2));

    let writer = {
        let (cache, stop, start) = (cache.clone(), stop.clone(), start.clone());
        thread::spawn(move || {
            start.wait();
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = (i as usize * 7919) % n;
                apply(
                    &cache,
                    Message {
                        key: format!("p-{id}"),
                        value: Some(make_value(id, i + 2)),
                    },
                );
                i += 1;
                thread::sleep(Duration::from_millis(1));
            }
        })
    };

    let handles: Vec<_> = (0..readers)
        .map(|t| {
            let (cache, stop, start) = (cache.clone(), stop.clone(), start.clone());
            thread::spawn(move || {
                let mut rnd = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                let mut samples = Vec::new();
                let mut count = 0u64;
                let mut max = 0u64;
                start.wait();
                while !stop.load(Ordering::Relaxed) {
                    let key = format!("p-{}", next_random(&mut rnd) % n as u64);
                    let t0 = Instant::now();
                    black_box(get(&cache, &key));
                    let ns = t0.elapsed().as_nanos() as u64;
                    max = max.max(ns);
                    if count.is_multiple_of(16) {
                        samples.push(ns);
                    }
                    count += 1;
                }
                (count, samples, max)
            })
        })
        .collect();

    start.wait();
    thread::sleep(Duration::from_secs_f64(secs));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let (mut total, mut all, mut max) = (0, Vec::new(), 0);
    for h in handles {
        let (c, s, m) = h.join().unwrap();
        total += c;
        all.extend(s);
        max = max.max(m);
    }
    (total, all, max)
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    sorted[((sorted.len() as f64 * q) as usize).min(sorted.len() - 1)]
}

fn bench(args: &[String]) {
    let arg = |name: &str, default: &str| -> String {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or(default.into())
    };
    let out = arg("--out", "results.csv");
    let run: u32 = arg("--run", "1").parse().unwrap();
    let n: usize = arg("--n", "1000000").parse().unwrap();
    let secs: f64 = arg("--secs", "3").parse().unwrap();
    let cores = thread::available_parallelism()
        .map(|c| c.get())
        .unwrap_or(4);

    let cache = Arc::new(Cache::new());
    let t0 = Instant::now();
    for id in 0..n {
        apply(
            &cache,
            Message {
                key: format!("p-{id}"),
                value: Some(make_value(id, 1)),
            },
        );
    }
    let warmup_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let rss_warm = rss_mb();
    println!("[rust] запуск {run}: прогрев {n} записей за {warmup_ms:.0} мс, RSS {rss_warm:.0} МБ");

    run_step(&cache, n, 1, 1.0);

    let new_file = std::fs::metadata(&out)
        .map(|m| m.len() == 0)
        .unwrap_or(true);
    let mut csv = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out)
        .unwrap();
    if new_file {
        writeln!(csv, "impl,run,threads,warmup_ms,rss_after_warmup_mb,mops,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,rss_mb,gc_count,gc_pause_ms").unwrap();
    }
    let mut readers = 1;
    loop {
        let (total, mut lat, max) = run_step(&cache, n, readers, secs);
        lat.sort_unstable();
        let mops = total as f64 / secs / 1e6;
        let (p50, p95, p99, p999) = (
            percentile(&lat, 0.5),
            percentile(&lat, 0.95),
            percentile(&lat, 0.99),
            percentile(&lat, 0.999),
        );
        let rss = rss_mb();
        println!("  читателей {readers:>2}: {mops:.2} млн get/с | p50 {p50} | p95 {p95} | p99 {p99} | p99.9 {p999} | max {max} нс | RSS {rss:.0} МБ");
        writeln!(csv, "rust,{run},{readers},{warmup_ms:.1},{rss_warm:.1},{mops:.4},{p50},{p95},{p99},{p999},{max},{rss:.1},0,0").unwrap();
        if readers >= cores {
            break;
        }
        readers = (readers * 2).min(cores);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("bench") {
        bench(&args[1..]);
    } else {
        demo();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_value_wins() {
        let cache = Cache::new();
        apply(&cache, msg("p-1", Some("old")));
        apply(&cache, msg("p-1", Some("new")));
        assert_eq!(get(&cache, "p-1"), Some(b"new".to_vec()));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn tombstone_deletes() {
        let cache = Cache::new();
        apply(&cache, msg("p-1", Some("x")));
        assert_eq!(apply(&cache, msg("p-1", None)), "удалила");
        assert_eq!(get(&cache, "p-1"), None);
    }

    #[test]
    fn answers_503_until_warmed_up() {
        let cache = Cache::new();
        let ready = AtomicBool::new(false);
        assert!(http_get(&cache, &ready, "p-1").starts_with("503"));
        warm_up(&cache, fake_kafka(), &ready, false);
        assert!(http_get(&cache, &ready, "p-1").contains("переезд"));
        assert!(http_get(&cache, &ready, "p-2").starts_with("404"));
        assert!(http_get(&cache, &ready, "p-3").starts_with("200"));
    }

    #[test]
    fn readers_never_go_back_in_time() {
        const N: u32 = 10_000;
        let cache = Arc::new(Cache::new());
        let start = Arc::new(Barrier::new(5));
        let writer = {
            let (cache, start) = (cache.clone(), start.clone());
            thread::spawn(move || {
                start.wait();
                for v in 1..=N {
                    apply(&cache, msg("p-1", Some(&v.to_string())));
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (cache, start) = (cache.clone(), start.clone());
                thread::spawn(move || {
                    start.wait();
                    let mut last = 0;
                    while last < N {
                        if let Some(v) = get(&cache, "p-1") {
                            let v: u32 = String::from_utf8(v).unwrap().parse().unwrap();
                            assert!(v >= last, "увидела {v} после {last}");
                            last = v;
                        }
                    }
                })
            })
            .collect();
        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }

    #[test]
    fn warm_up_reads_every_partition_to_the_end() {
        let cache = Cache::new();
        let mut parts = fake_kafka();
        parts.push(
            (0..100)
                .map(|i| msg(&format!("x-{i}"), Some("{}")))
                .collect(),
        );
        warm_up(&cache, parts, &AtomicBool::new(false), false);
        assert!((0..100).all(|i| get(&cache, &format!("x-{i}")).is_some()));
    }
}
