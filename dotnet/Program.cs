using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;

switch (args.FirstOrDefault())
{
    case "test": return Checks.Run();
    case "bench": Bench.Run(args.Skip(1).ToArray()); return 0;
    default: Cache.Demo(); return 0;
}
record Message(string Key, byte[]? Value)
{
    public static Message Of(string key, string? value) =>
        new(key, value is null ? null : Encoding.UTF8.GetBytes(value));
}

class ReadyFlag
{
    private volatile bool _set;
    public bool IsSet => _set;
    public void Set() => _set = true;
}

static class Cache
{
    public static ConcurrentDictionary<string, byte[]> New() => new(StringComparer.Ordinal);

    public static string Apply(ConcurrentDictionary<string, byte[]> cache, Message m)
    {
        if (m.Value is null)
            return cache.TryRemove(m.Key, out _) ? "удалила" : "удалять нечего";
        if (cache.TryAdd(m.Key, m.Value)) return "вставила";
        cache[m.Key] = m.Value; // старый массив станет мусором — его соберёт GC
        return "заменила";
    }

    public static byte[]? Get(ConcurrentDictionary<string, byte[]> cache, string key) =>
        cache.TryGetValue(key, out var v) ? v.AsSpan().ToArray() : null;

    public static TimeSpan WarmUp(ConcurrentDictionary<string, byte[]> cache, List<List<Message>> partitions,
                                  ReadyFlag ready, bool print)
    {
        var sw = Stopwatch.StartNew();
        var high = partitions.Select(p => p.Count).ToArray();
        var position = new int[partitions.Count];
        while (position.Where((pos, i) => pos < high[i]).Any())
        {
            for (var p = 0; p < partitions.Count; p++)
            {
                if (position[p] >= high[p]) continue;
                var m = partitions[p][position[p]];
                var what = Apply(cache, m);
                if (print) Console.WriteLine($"  партиция {p}, offset {position[p]}: {m.Key} -> {what}");
                position[p]++;
            }
        }
        var took = sw.Elapsed;
        ready.Set();
        return took;
    }

    public static string HttpGet(ConcurrentDictionary<string, byte[]> cache, ReadyFlag ready, string key)
    {
        if (!ready.IsSet) return "503 cache is warming up";
        var v = Get(cache, key);
        return v is null ? "404 not found" : $"200 {Encoding.UTF8.GetString(v)}";
    }

    public static List<List<Message>> FakeKafka() => new()
    {
        new() { Message.Of("p-1", """{"name":"Сыктывкар-1","version":1}"""),
                Message.Of("p-1", """{"name":"Сыктывкар-1 (переезд)","version":2}""") },
        new() { Message.Of("p-2", """{"name":"Москва-7","version":1}"""),
                Message.Of("p-2", null) }, // tombstone: пиццерию закрыли
        new() { Message.Of("p-3", """{"name":"Казань-2","version":1}""") },
    };

    public static void Demo()
    {
        var cache = New();
        var ready = new ReadyFlag();
        Console.WriteLine($"до прогрева: GET p-1 -> {HttpGet(cache, ready, "p-1")}");
        Console.WriteLine("== прогрев: читаю топик с начала");
        var took = WarmUp(cache, FakeKafka(), ready, print: true);
        Console.WriteLine($"== прогрев закончен за {took.TotalMilliseconds:F3} мс");
        foreach (var key in new[] { "p-1", "p-2", "p-3", "p-404" })
            Console.WriteLine($"  GET {key} -> {HttpGet(cache, ready, key)}");
        Console.WriteLine($"новое сообщение после старта: p-3 -> {Apply(cache, Message.Of("p-3", """{"name":"Казань-2 (ремонт)","version":2}"""))}");
        Console.WriteLine($"  GET p-3 -> {HttpGet(cache, ready, "p-3")}");
    }
}

// дополнительно считаем сборки мусора и паузы GC
static class Bench
{
    static byte[] MakeValue(long id, ulong version) => Encoding.UTF8.GetBytes(
        $$"""{"id":"p-{{id}}","name":"Пиццерия номер {{id}}","city":"Сыктывкар","address":"ул. Ленина, {{id}}","open":true,"version":{{version}}}""");

    static double RssMb() => Environment.WorkingSet / 1048576.0;

    static ulong NextRandom(ref ulong s)
    {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        return s;
    }

    static (long Total, List<long> Samples, long Max) RunStep(ConcurrentDictionary<string, byte[]> cache, int n, int readers, double secs)
    {
        var stop = false;
        using var start = new Barrier(readers + 2);
        var nsPerTick = 1e9 / Stopwatch.Frequency;

        var writer = new Thread(() =>
        {
            start.SignalAndWait();
            ulong i = 0;
            while (!Volatile.Read(ref stop))
            {
                var id = (long)(i * 7919 % (ulong)n);
                Cache.Apply(cache, new Message($"p-{id}", MakeValue(id, i + 2)));
                i++;
                Thread.Sleep(1);
            }
        });

        var results = new (long Count, List<long> Samples, long Max)[readers];
        var threads = Enumerable.Range(0, readers).Select(t => new Thread(() =>
        {
            var rnd = 0x9E3779B97F4A7C15UL ^ (ulong)(t + 1);
            var samples = new List<long>();
            long count = 0, sink = 0, max = 0;
            start.SignalAndWait();
            while (!Volatile.Read(ref stop))
            {
                var key = $"p-{NextRandom(ref rnd) % (ulong)n}";
                var t0 = Stopwatch.GetTimestamp();
                sink += Cache.Get(cache, key)?.Length ?? 0;
                var ns = (long)((Stopwatch.GetTimestamp() - t0) * nsPerTick);
                if (ns > max) max = ns;
                if (count % 16 == 0) samples.Add(ns);
                count++;
            }
            GC.KeepAlive(sink);
            results[t] = (count, samples, max);
        })).ToList();

        writer.Start();
        threads.ForEach(th => th.Start());
        start.SignalAndWait();
        Thread.Sleep(TimeSpan.FromSeconds(secs));
        Volatile.Write(ref stop, true);
        threads.ForEach(th => th.Join());
        writer.Join();
        return (results.Sum(r => r.Count), results.SelectMany(r => r.Samples).ToList(), results.Max(r => r.Max));
    }

    static long Percentile(List<long> sorted, double q) => sorted[Math.Min((int)(sorted.Count * q), sorted.Count - 1)];

    public static void Run(string[] args)
    {
        string Arg(string name, string def) { var i = Array.IndexOf(args, name); return i >= 0 && i + 1 < args.Length ? args[i + 1] : def; }
        var inv = CultureInfo.InvariantCulture;
        var outPath = Arg("--out", "results.csv");
        var run = int.Parse(Arg("--run", "1"));
        var n = int.Parse(Arg("--n", "1000000"));
        var secs = double.Parse(Arg("--secs", "3"), inv);
        var cores = Environment.ProcessorCount;
        var cache = Cache.New();
        var sw = Stopwatch.StartNew();
        for (var id = 0; id < n; id++) Cache.Apply(cache, new Message($"p-{id}", MakeValue(id, 1)));
        var warmupMs = sw.Elapsed.TotalMilliseconds;
        var rssWarm = RssMb();
        Console.WriteLine($"[dotnet] запуск {run}: прогрев {n} записей за {warmupMs:F0} мс, RSS {rssWarm:F0} МБ");

        RunStep(cache, n, 1, 1.0);

        var newFile = !File.Exists(outPath) || new FileInfo(outPath).Length == 0;
        using var csv = new StreamWriter(outPath, append: true);
        if (newFile) csv.WriteLine("impl,run,threads,warmup_ms,rss_after_warmup_mb,mops,p50_ns,p95_ns,p99_ns,p999_ns,max_ns,rss_mb,gc_count,gc_pause_ms");
        for (var readers = 1; ; readers = Math.Min(readers * 2, cores))
        {
            var gcBefore = GC.CollectionCount(0);
            var pauseBefore = GC.GetTotalPauseDuration();
            var (total, lat, max) = RunStep(cache, n, readers, secs);
            var gcCount = GC.CollectionCount(0) - gcBefore; // gen0 считается при любой сборке
            var pauseMs = (GC.GetTotalPauseDuration() - pauseBefore).TotalMilliseconds;

            lat.Sort();
            var mops = total / secs / 1e6;
            long p50 = Percentile(lat, 0.5), p95 = Percentile(lat, 0.95), p99 = Percentile(lat, 0.99), p999 = Percentile(lat, 0.999);
            var rss = RssMb();
            Console.WriteLine($"  читателей {readers,2}: {mops:F2} млн get/с | p50 {p50} | p95 {p95} | p99 {p99} | p99.9 {p999} | max {max} нс | RSS {rss:F0} МБ | GC: {gcCount} сборок, паузы {pauseMs:F1} мс");
            csv.WriteLine(string.Join(",", "dotnet", run, readers, warmupMs.ToString("F1", inv), rssWarm.ToString("F1", inv),
                mops.ToString("F4", inv), p50, p95, p99, p999, max, rss.ToString("F1", inv), gcCount, pauseMs.ToString("F1", inv)));
            if (readers >= cores) break;
        }
    }
}

static class Checks
{
    public static int Run()
    {
        var tests = new (string Name, Action Body)[]
        {
            ("last_value_wins", LastValueWins),
            ("tombstone_deletes", TombstoneDeletes),
            ("answers_503_until_warmed_up", Answers503UntilWarmedUp),
            ("readers_never_go_back_in_time", ReadersNeverGoBackInTime),
            ("warm_up_reads_every_partition_to_the_end", WarmUpReadsEveryPartitionToTheEnd),
        };
        var failed = 0;
        foreach (var (name, body) in tests)
        {
            try { body(); Console.WriteLine($"test {name} ... ok"); }
            catch (Exception e) { failed++; Console.WriteLine($"test {name} ... FAILED: {e.Message}"); }
        }
        Console.WriteLine($"test result: {(failed == 0 ? "ok" : "FAILED")}. {tests.Length - failed} passed; {failed} failed");
        return failed == 0 ? 0 : 1;
    }

    static void Assert(bool ok, string message) { if (!ok) throw new Exception(message); }
    static string? GetText(ConcurrentDictionary<string, byte[]> c, string k) => Cache.Get(c, k) is { } v ? Encoding.UTF8.GetString(v) : null;

    static void LastValueWins()
    {
        var cache = Cache.New();
        Cache.Apply(cache, Message.Of("p-1", "old"));
        Cache.Apply(cache, Message.Of("p-1", "new"));
        Assert(GetText(cache, "p-1") == "new", "должно остаться последнее значение");
        Assert(cache.Count == 1, "ключ один — запись одна");
    }

    static void TombstoneDeletes()
    {
        var cache = Cache.New();
        Cache.Apply(cache, Message.Of("p-1", "x"));
        Assert(Cache.Apply(cache, Message.Of("p-1", null)) == "удалила", "tombstone должен удалить");
        Assert(Cache.Get(cache, "p-1") is null, "после tombstone записи нет");
    }

    static void Answers503UntilWarmedUp()
    {
        var cache = Cache.New();
        var ready = new ReadyFlag();
        Assert(Cache.HttpGet(cache, ready, "p-1").StartsWith("503"), "до прогрева — 503");
        Cache.WarmUp(cache, Cache.FakeKafka(), ready, print: false);
        Assert(Cache.HttpGet(cache, ready, "p-1").Contains("переезд"), "последняя версия p-1");
        Assert(Cache.HttpGet(cache, ready, "p-2").StartsWith("404"), "p-2 удалена tombstone");
        Assert(Cache.HttpGet(cache, ready, "p-3").StartsWith("200"), "p-3 есть");
    }

    // Писатель пишет версии 1..N, 4 читателя одновременно читают (старт по Barrier).
    static void ReadersNeverGoBackInTime()
    {
        const int n = 10_000;
        var cache = Cache.New();
        using var start = new Barrier(5);
        string? error = null;
        var writer = new Thread(() =>
        {
            start.SignalAndWait();
            for (var v = 1; v <= n; v++) Cache.Apply(cache, Message.Of("p-1", v.ToString()));
        });
        var readers = Enumerable.Range(0, 4).Select(_ => new Thread(() =>
        {
            start.SignalAndWait();
            var last = 0;
            while (last < n)
            {
                var s = GetText(cache, "p-1");
                if (s is null) continue;
                var v = int.Parse(s);
                if (v < last) { error = $"увидела {v} после {last}"; return; }
                last = v;
            }
        })).ToList();
        writer.Start();
        readers.ForEach(t => t.Start());
        writer.Join();
        readers.ForEach(t => t.Join());
        Assert(error is null, error ?? "");
    }

    static void WarmUpReadsEveryPartitionToTheEnd()
    {
        var cache = Cache.New();
        var parts = Cache.FakeKafka();
        parts.Add(Enumerable.Range(0, 100).Select(i => Message.Of($"x-{i}", "{}")).ToList());
        Cache.WarmUp(cache, parts, new ReadyFlag(), print: false);
        Assert(Enumerable.Range(0, 100).All(i => Cache.Get(cache, $"x-{i}") is not null), "все записи длинной партиции");
    }
}
