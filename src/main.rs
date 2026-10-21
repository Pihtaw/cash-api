// Кеш на простых функциях: учебный пример.
//
// Идея: в будущем сервисе данные будут приходить из Kafka, а здесь
// вызываем функции кеша вручную и проверяю, что он ведёт себя правильно.
//
// Запуск:  cargo run
// Тесты:   cargo test

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

// Кеш — это словарь «ключ -> значение» под блокировкой чтения-записи.
// `type` просто даёт короткое имя длинному типу, нового типа не создаёт.
type Cache = RwLock<HashMap<String, String>>;

// Создать пустой кеш.
fn new_cache() -> Cache {
    RwLock::new(HashMap::new())
}

// Положить значение. Если ключ уже был, старое значение заменится.
fn put(cache: &Cache, key: &str, value: &str) {
    // write() — блокировка на запись: пока она у меня, никто не читает и не пишет.
    let mut map = cache.write().unwrap();
    map.insert(key.to_string(), value.to_string());
    // здесь map заканчивается, и блокировка снимается сама
}

// Получить значение. None, если такого ключа нет.
fn get(cache: &Cache, key: &str) -> Option<String> {
    // read() — блокировка на чтение: читать одновременно могут многие потоки.
    let map = cache.read().unwrap();
    // map.get возвращает ссылку на значение внутри словаря.
    // cloned() делает копию, потому что ссылку нельзя отдать за пределы блокировки.
    map.get(key).cloned()
}

// Удалить значение. Возвращает true, если ключ был.
fn delete(cache: &Cache, key: &str) -> bool {
    let mut map = cache.write().unwrap();
    map.remove(key).is_some()
}

// Сколько записей в кеше.
fn len(cache: &Cache) -> usize {
    cache.read().unwrap().len()
}

// Применить «сообщение из Kafka»: есть значение — положить, нет (tombstone) — удалить.
fn apply(cache: &Cache, key: &str, value: Option<&str>) {
    match value {
        Some(v) => put(cache, key, v),
        None => {
            delete(cache, key);
        }
    }
}

fn main() {
    println!("--- 1. Один поток: базовые операции ---");
    let cache = new_cache();

    put(&cache, "p-1", r#"{"name":"Сыктывкар-1"}"#);
    put(&cache, "p-2", r#"{"name":"Москва-7"}"#);
    println!("после двух put: {} записи", len(&cache));

    put(&cache, "p-1", r#"{"name":"Сыктывкар-1 (переезд)"}"#);
    println!("p-1 после обновления: {:?}", get(&cache, "p-1"));

    apply(&cache, "p-2", None); // tombstone
    println!("p-2 после tombstone: {:?}", get(&cache, "p-2"));
    println!("p-3 (не было): {:?}", get(&cache, "p-3"));
    println!("итого записей: {}", len(&cache));

    println!();
    println!("--- 2. Несколько потоков: один пишет, трое читают ---");
    // Arc — чтобы кешем могли владеть сразу несколько потоков.
    let cache = Arc::new(new_cache());

    let writer = {
        let cache = Arc::clone(&cache);
        thread::spawn(move || {
            for version in 1..=5 {
                put(&cache, "p-1", &format!("version {version}"));
            }
        })
    };

    let mut readers = Vec::new();
    for id in 1..=3 {
        let cache = Arc::clone(&cache);
        readers.push(thread::spawn(move || {
            // Читатель может попасть на любой момент записи: увидеть
            // «ещё ничего», version 1, ..., version 5 — но никогда
            // не увидит «половину» значения.
            println!("читатель {id} видит: {:?}", get(&cache, "p-1"));
        }));
    }

    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    println!("в конце: {:?}", get(&cache, "p-1"));
}

// -----------------------------------------------------------------------------
// Тесты: каждый проверяет одно правило работы кеша.
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cache_has_nothing() {
        let cache = new_cache();
        assert_eq!(len(&cache), 0);
        assert_eq!(get(&cache, "p-1"), None);
    }

    #[test]
    fn put_then_get() {
        let cache = new_cache();
        put(&cache, "p-1", "A");
        assert_eq!(get(&cache, "p-1"), Some("A".to_string()));
    }

    #[test]
    fn last_value_wins() {
        let cache = new_cache();
        put(&cache, "p-1", "old");
        put(&cache, "p-1", "new");
        assert_eq!(get(&cache, "p-1"), Some("new".to_string()));
        assert_eq!(len(&cache), 1, "ключ один, записей тоже одна");
    }

    #[test]
    fn keys_do_not_mix() {
        let cache = new_cache();
        put(&cache, "p-1", "A");
        put(&cache, "p-2", "B");
        assert_eq!(get(&cache, "p-1"), Some("A".to_string()));
        assert_eq!(get(&cache, "p-2"), Some("B".to_string()));
    }

    #[test]
    fn delete_removes_key() {
        let cache = new_cache();
        put(&cache, "p-1", "A");
        assert!(delete(&cache, "p-1"));
        assert_eq!(get(&cache, "p-1"), None);
    }

    #[test]
    fn delete_missing_key_is_ok() {
        let cache = new_cache();
        assert!(
            !delete(&cache, "nope"),
            "удаление несуществующего ключа не ошибка"
        );
    }

    #[test]
    fn tombstone_deletes() {
        let cache = new_cache();
        apply(&cache, "p-1", Some("A"));
        apply(&cache, "p-1", None);
        assert_eq!(get(&cache, "p-1"), None);
    }

    #[test]
    fn keys_are_case_sensitive() {
        // В отличие от StringComparer.OrdinalIgnoreCase в C#, по умолчанию
        // "P-1" и "p-1" — разные ключи.
        let cache = new_cache();
        put(&cache, "p-1", "A");
        assert_eq!(get(&cache, "P-1"), None);
    }

    #[test]
    fn readers_never_see_old_value_after_new_one() {
        // Один писатель пишет версии 1..1000 по порядку, читатели читают параллельно.
        // Каждый читатель должен видеть версии только неубывающими:
        // если увидел 500, то потом не может увидеть 499.
        let cache = Arc::new(new_cache());

        let writer = {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for v in 1..=1000 {
                    put(&cache, "p-1", &v.to_string());
                }
            })
        };

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    let mut last = 0;
                    for _ in 0..1000 {
                        if let Some(s) = get(&cache, "p-1") {
                            let v: u32 = s.parse().unwrap();
                            assert!(v >= last, "увидели {v} после {last}");
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
        assert_eq!(get(&cache, "p-1"), Some("1000".to_string()));
    }
}
