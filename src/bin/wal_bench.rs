use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use wal::{
    config::{
        DEFAULT_WRITE_BUFFER_SIZE, RECORD_HEADER_LEN, SEGMENT_HEADER_LEN, SyncPolicy, WalConfig,
    },
    io::directory::{FsSegmentDirectory, SegmentDirectory},
    lsn::Lsn,
    types::{RecordType, WalIdentity, record_types},
    wal::{WalHandle, metrics::WalMetrics},
};

type BenchResult<T> = Result<T, Box<dyn Error>>;

#[derive(Debug, Clone)]
struct Args {
    dir: PathBuf,
    records: usize,
    sync_records: usize,
    payload_size: usize,
    batch_size: usize,
    threads: usize,
    point_reads: usize,
}

#[derive(Debug)]
struct BenchRow {
    name: String,
    records: u64,
    payload_bytes: u64,
    wal_bytes: u64,
    duration: Duration,
    latencies_ns: Vec<u64>,
    notes: String,
}

#[derive(Debug)]
struct WrittenWal {
    config: WalConfig,
    lsns: Vec<Lsn>,
    last_lsn: Option<Lsn>,
    metrics: WalMetrics,
}

fn main() -> BenchResult<()> {
    let args = Args::parse()?;
    fs::create_dir_all(&args.dir)?;

    println!("# WAL Benchmark Report\n");
    println!("Generated unix seconds: {}", now_secs());
    println!("OS/arch: {}/{}", env::consts::OS, env::consts::ARCH);
    println!(
        "CPU threads visible: {}",
        thread::available_parallelism()?.get()
    );
    println!("Benchmark root: `{}`", args.dir.display());
    println!("Records: {}", args.records);
    println!("Sync records: {}", args.sync_records);
    println!("Payload size: {} bytes", args.payload_size);
    println!("Batch size: {}", args.batch_size);
    println!("Writer threads: {}", args.threads);
    println!("Point reads: {}", args.point_reads);
    println!();

    let mut rows = Vec::new();

    rows.push(bench_append_no_sync(&args)?);
    rows.push(bench_sync_every_record(&args)?);
    rows.push(bench_batch_append_sync(&args)?);
    rows.push(bench_concurrent_sync_through(&args)?);
    rows.push(bench_rollover(&args)?);
    rows.push(bench_recovery_scan(&args)?);
    rows.push(bench_iterator_replay(&args)?);
    rows.push(bench_point_reads(&args)?);
    rows.push(bench_retention_prune(&args)?);
    rows.push(bench_tail_follow(&args)?);
    rows.push(bench_corrupt_tail_repair(&args)?);
    rows.push(bench_clean_shutdown_reopen(&args)?);

    print_rows(&rows);

    println!("\n## Notes\n");
    println!("- `append-no-sync` measures append path cost without durable commit per record.");
    println!("- `sync-every-record` is the worst-case durable commit path.");
    println!("- `batch-append-sync` shows how much batching improves fsync amortization.");
    println!("- `concurrent-sync-through` exercises your current group-commit style API.");
    println!(
        "- `recovery-scan`, `iterator-replay`, `corrupt-tail-repair`, and `clean-shutdown-reopen` are the most README-worthy correctness/performance numbers."
    );
    println!(
        "- Run on an otherwise idle machine, with `cargo run --release`, and keep the benchmark directory on the disk you care about."
    );

    Ok(())
}

impl Args {
    fn parse() -> BenchResult<Self> {
        let mut args = Self {
            dir: PathBuf::from("/tmp/wal-bench"),
            records: 50_000,
            sync_records: 2_000,
            payload_size: 1024,
            batch_size: 128,
            threads: thread::available_parallelism()?.get().min(8).max(1),
            point_reads: 10_000,
        };

        let mut it = env::args().skip(1);
        while let Some(flag) = it.next() {
            match flag.as_str() {
                "--dir" => args.dir = PathBuf::from(required(&mut it, "--dir")?),
                "--records" => args.records = parse_usize(&mut it, "--records")?,
                "--sync-records" => args.sync_records = parse_usize(&mut it, "--sync-records")?,
                "--payload-size" => args.payload_size = parse_usize(&mut it, "--payload-size")?,
                "--batch-size" => args.batch_size = parse_usize(&mut it, "--batch-size")?.max(1),
                "--threads" => args.threads = parse_usize(&mut it, "--threads")?.max(1),
                "--point-reads" => args.point_reads = parse_usize(&mut it, "--point-reads")?,
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}").into()),
            }
        }

        if args.payload_size > u32::MAX as usize {
            return Err("payload size must fit in u32".into());
        }

        args.records = args.records.max(1);
        args.sync_records = args.sync_records.min(args.records).max(1);

        Ok(args)
    }
}

fn required(it: &mut impl Iterator<Item = String>, flag: &str) -> BenchResult<String> {
    it.next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn parse_usize(it: &mut impl Iterator<Item = String>, flag: &str) -> BenchResult<usize> {
    Ok(required(it, flag)?.parse()?)
}

fn print_help() {
    println!(
        "Usage:
  cargo run --release --bin wal_bench -- [options]

Options:
  --dir PATH              Benchmark root directory [default: /tmp/wal-bench]
  --records N             Records for bulk scenarios [default: 50000]
  --sync-records N        Records for fsync-heavy scenarios [default: 2000]
  --payload-size N        Payload bytes per user record [default: 1024]
  --batch-size N          Batch size for append_batch [default: 128]
  --threads N             Writer threads for sync_through test [default: min(cpu, 8)]
  --point-reads N         Point read operations [default: 10000]"
    );
}

fn bench_append_no_sync(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "append-no-sync")?;
    let config = config_for(&dir, args.payload_size, None);
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 1);
    let mut latencies = Vec::with_capacity(args.records);

    let started = Instant::now();
    for _ in 0..args.records {
        let op = Instant::now();
        wal.append(RecordType::new(record_types::USER_MIN), &payload)?;
        latencies.push(ns(op.elapsed()));
    }
    let duration = started.elapsed();
    wal.sync()?;

    let metrics = wal.metrics();
    Ok(row(
        "append-no-sync",
        args.records,
        args.payload_size,
        duration,
        latencies,
        &metrics,
        "final sync excluded from timed loop",
    ))
}

fn bench_sync_every_record(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "sync-every-record")?;
    let config = config_for(&dir, args.payload_size, None);
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 2);
    let mut latencies = Vec::with_capacity(args.sync_records);

    let started = Instant::now();
    for _ in 0..args.sync_records {
        let op = Instant::now();
        wal.append(RecordType::new(record_types::USER_MIN), &payload)?;
        wal.sync()?;
        latencies.push(ns(op.elapsed()));
    }
    let duration = started.elapsed();

    let metrics = wal.metrics();
    Ok(row(
        "sync-every-record",
        args.sync_records,
        args.payload_size,
        duration,
        latencies,
        &metrics,
        "append + sync per record",
    ))
}

fn bench_batch_append_sync(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "batch-append-sync")?;
    let config = config_for(&dir, args.payload_size, None);
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 3);
    let mut latencies = Vec::new();

    let started = Instant::now();
    let mut written = 0usize;
    while written < args.records {
        let n = args.batch_size.min(args.records - written);
        let batch = repeated_batch(n, payload.as_slice());

        let op = Instant::now();
        wal.append_batch(&batch)?;
        wal.sync()?;
        latencies.push(ns(op.elapsed()));

        written += n;
    }
    let duration = started.elapsed();

    let metrics = wal.metrics();
    Ok(row(
        "batch-append-sync",
        args.records,
        args.payload_size,
        duration,
        latencies,
        &metrics,
        "latency is per batch",
    ))
}

fn bench_concurrent_sync_through(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "concurrent-sync-through")?;
    let config = config_for(&dir, args.payload_size, None);
    let (wal, _) = open_handle(config.clone())?;
    let wal = Arc::new(wal);

    let per_thread = (args.sync_records / args.threads).max(1);
    let total_records = per_thread * args.threads;
    let record_len = physical_record_len(args.payload_size, config.record_alignment);
    let barrier = Arc::new(Barrier::new(args.threads + 1));

    let mut joins = Vec::new();
    for tid in 0..args.threads {
        let wal = Arc::clone(&wal);
        let barrier = Arc::clone(&barrier);
        let payload = make_payload(args.payload_size, 100 + tid as u64);

        joins.push(thread::spawn(move || -> Result<Vec<u64>, String> {
            let mut latencies = Vec::with_capacity(per_thread);
            barrier.wait();

            for _ in 0..per_thread {
                let op = Instant::now();
                let lsn = wal
                    .append(RecordType::new(record_types::USER_MIN), &payload)
                    .map_err(|e| e.to_string())?;
                let end_lsn = lsn
                    .checked_add_bytes(record_len)
                    .ok_or_else(|| "lsn overflow".to_string())?;
                wal.sync_through(end_lsn).map_err(|e| e.to_string())?;
                latencies.push(ns(op.elapsed()));
            }

            Ok(latencies)
        }));
    }

    let started = Instant::now();
    barrier.wait();

    let mut latencies = Vec::with_capacity(total_records);
    for join in joins {
        latencies.extend(join.join().map_err(|_| "writer thread panicked")??);
    }

    let duration = started.elapsed();
    let metrics = wal.metrics();
    Ok(row(
        "concurrent-sync-through",
        total_records,
        args.payload_size,
        duration,
        latencies,
        &metrics,
        "append + sync_through from multiple threads",
    ))
}

fn bench_rollover(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "rollover")?;
    let realistic_segment_size = 16 * 1024 * 1024;
    let min_segment_size = min_valid_segment_size(args.payload_size.max(1), 0);
    let target_segment_size = realistic_segment_size.max(min_segment_size);

    let record_len = physical_record_len(args.payload_size.max(1), 0);
    let records_for_four_segments = ((target_segment_size * 4) / record_len) as usize;

    let records = args.records.max(records_for_four_segments);
    let config = config_for(&dir, args.payload_size, Some(target_segment_size));
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 4);
    let mut latencies = Vec::with_capacity(records);

    let started = Instant::now();
    for _ in 0..records {
        let op = Instant::now();
        wal.append(RecordType::new(record_types::USER_MIN), &payload)?;
        latencies.push(ns(op.elapsed()));
    }
    let duration = started.elapsed();

    wal.sync()?;

    let metrics = wal.metrics();
    Ok(row(
        "rollover",
        records,
        args.payload_size,
        duration,
        latencies,
        &metrics,
        "realistic 16MiB segment rollover workload",
    ))
}

fn bench_recovery_scan(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "recovery-scan")?;
    let written = write_records(&dir, args, args.records, None, false)?;

    let started = Instant::now();
    let (_wal, report) = open_handle(written.config.clone())?;
    let duration = started.elapsed();

    Ok(BenchRow {
        name: "recovery-scan".to_string(),
        records: report.records_scanned,
        payload_bytes: args.records as u64 * args.payload_size as u64,
        wal_bytes: written.metrics.current_wal_size,
        duration,
        latencies_ns: Vec::new(),
        notes: format!(
            "segments_scanned={}, sealed_segments={}, corrupt_records={}, truncated_bytes={}, internal_recovery_duration_ms={:.3}",
            report.segments_scanned,
            report.sealed_segments,
            report.corrupt_records_found,
            report.truncated_bytes,
            ms(report.recovery_duration),
        ),
    })
}

fn bench_iterator_replay(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "iterator-replay")?;
    let written = write_records(&dir, args, args.records, None, false)?;
    let (wal, _) = open_handle(written.config.clone())?;

    let started = Instant::now();
    let mut iter = wal.iter_from(Lsn::ZERO)?;
    let mut records = 0u64;
    let mut payload_bytes = 0u64;

    while let Some(record) = iter.next()? {
        if record.record_type.is_user_defined() {
            records += 1;
            payload_bytes += record.payload.len() as u64;
        }
    }

    let duration = started.elapsed();

    Ok(BenchRow {
        name: "iterator-replay".to_string(),
        records,
        payload_bytes,
        wal_bytes: written.metrics.current_wal_size,
        duration,
        latencies_ns: Vec::new(),
        notes: "sequential iter_from(0) replay".to_string(),
    })
}

fn bench_point_reads(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "point-reads")?;
    let written = write_records(&dir, args, args.records, None, true)?;
    let (wal, _) = open_handle(written.config.clone())?;

    let reads = args.point_reads.max(1);
    let mut rng = 0x1234_5678_90ab_cdefu64;
    let mut latencies = Vec::with_capacity(reads);

    let started = Instant::now();
    for _ in 0..reads {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (rng as usize) % written.lsns.len();

        let op = Instant::now();
        let record = wal.read_at(written.lsns[idx])?;
        if !record.record_type.is_user_defined() {
            return Err("point read returned an internal record unexpectedly".into());
        }
        latencies.push(ns(op.elapsed()));
    }

    let duration = started.elapsed();

    Ok(BenchRow {
        name: "point-reads".to_string(),
        records: reads as u64,
        payload_bytes: reads as u64 * args.payload_size as u64,
        wal_bytes: written.metrics.current_wal_size,
        duration,
        latencies_ns: latencies,
        notes: "random read_at(lsn) over collected user LSNs".to_string(),
    })
}

fn bench_retention_prune(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "retention-prune")?;
    let segment_size = small_segment_size(args.payload_size, 8);
    let written = write_records(&dir, args, args.records.max(32), Some(segment_size), true)?;
    let last_lsn = written.last_lsn.ok_or("no last lsn")?;

    let (wal, _) = open_handle(written.config.clone())?;
    let size_before = wal.current_wal_size();

    let started = Instant::now();
    let removed = wal.truncate_segments_before(last_lsn)?;
    let duration = started.elapsed();

    let size_after = wal.current_wal_size();

    Ok(BenchRow {
        name: "retention-prune".to_string(),
        records: removed as u64,
        payload_bytes: 0,
        wal_bytes: size_before.saturating_sub(size_after),
        duration,
        latencies_ns: Vec::new(),
        notes: format!(
            "removed_segments={}, size_before={}, size_after={}",
            removed, size_before, size_after
        ),
    })
}

fn bench_tail_follow(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "tail-follow")?;
    let records = args.sync_records.min(args.records).max(1);
    let payload_size = args.payload_size.max(8);
    let config = config_for(&dir, payload_size, None);
    let (wal, _) = open_handle(config.clone())?;

    let mut tail = wal.tail_from(Lsn::ZERO)?;
    let wal_for_writer = wal.clone();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = Arc::clone(&barrier);
    let stamps: Arc<Vec<AtomicU64>> = Arc::new((0..records).map(|_| AtomicU64::new(0)).collect());
    let writer_stamps = Arc::clone(&stamps);
    let bench_origin = Instant::now();

    let writer = thread::spawn(move || -> Result<(), String> {
        writer_barrier.wait();

        for seq in 0..records {
            let mut payload = make_payload(payload_size, seq as u64);
            payload[..8].copy_from_slice(&(seq as u64).to_le_bytes());

            writer_stamps[seq].store(ns(bench_origin.elapsed()), Ordering::Release);
            wal_for_writer
                .append(RecordType::new(record_types::USER_MIN), &payload)
                .map_err(|e| e.to_string())?;
        }

        wal_for_writer.sync().map_err(|e| e.to_string())?;
        Ok(())
    });

    let mut latencies = Vec::with_capacity(records);
    let started = Instant::now();
    barrier.wait();

    while latencies.len() < records {
        let record = tail.next_blocking(Duration::from_secs(10))?;
        let Some(record) = record else {
            return Err("tail iterator timed out".into());
        };

        if !record.record_type.is_user_defined() {
            continue;
        }

        let mut seq_bytes = [0u8; 8];
        seq_bytes.copy_from_slice(&record.payload[..8]);
        let seq = u64::from_le_bytes(seq_bytes) as usize;

        if seq < records {
            let appended_at = stamps[seq].load(Ordering::Acquire);
            let now = ns(bench_origin.elapsed());
            latencies.push(now.saturating_sub(appended_at));
        }
    }

    writer.join().map_err(|_| "tail writer panicked")??;
    let duration = started.elapsed();

    let metrics = wal.metrics();
    Ok(row(
        "tail-follow",
        records,
        payload_size,
        duration,
        latencies,
        &metrics,
        "append-to-tail visibility latency",
    ))
}

fn bench_corrupt_tail_repair(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "corrupt-tail-repair")?;
    let written = write_records(&dir, args, args.records.max(2), None, true)?;
    let last_lsn = written.last_lsn.ok_or("no last lsn")?;

    corrupt_record_at(&dir, last_lsn, args.payload_size)?;

    let started = Instant::now();
    let (_wal, report) = open_handle(written.config.clone())?;
    let duration = started.elapsed();

    Ok(BenchRow {
        name: "corrupt-tail-repair".to_string(),
        records: report.records_scanned,
        payload_bytes: args.records as u64 * args.payload_size as u64,
        wal_bytes: written.metrics.current_wal_size,
        duration,
        latencies_ns: Vec::new(),
        notes: format!(
            "corrupt_records={}, truncated_bytes={}, next_lsn={}",
            report.corrupt_records_found,
            report.truncated_bytes,
            report.next_lsn.as_u64(),
        ),
    })
}

fn bench_clean_shutdown_reopen(args: &Args) -> BenchResult<BenchRow> {
    let dir = fresh_dir(args, "clean-shutdown-reopen")?;
    let config = config_for(&dir, args.payload_size, None);
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 5);

    let mut written = 0usize;
    while written < args.records {
        let n = args.batch_size.min(args.records - written);
        let batch = repeated_batch(n, payload.as_slice());
        wal.append_batch(&batch)?;
        written += n;
    }

    wal.sync()?;

    let shutdown_started = Instant::now();
    wal.shutdown()?;
    let shutdown_duration = shutdown_started.elapsed();
    drop(wal);

    let reopen_started = Instant::now();
    let (_wal, report) = open_handle(config.clone())?;
    let reopen_duration = reopen_started.elapsed();

    Ok(BenchRow {
        name: "clean-shutdown-reopen".to_string(),
        records: report.records_scanned,
        payload_bytes: args.records as u64 * args.payload_size as u64,
        wal_bytes: 0,
        duration: reopen_duration,
        latencies_ns: Vec::new(),
        notes: format!(
            "shutdown_ms={:.3}, recovery_skipped={}, clean_shutdown={}, segments_scanned={}",
            ms(shutdown_duration),
            report.recovery_skipped,
            report.clean_shutdown,
            report.segments_scanned,
        ),
    })
}

fn write_records(
    dir: &Path,
    args: &Args,
    records: usize,
    target_segment_size: Option<u64>,
    collect_lsns: bool,
) -> BenchResult<WrittenWal> {
    let config = config_for(dir, args.payload_size, target_segment_size);
    let (wal, _) = open_handle(config.clone())?;
    let payload = make_payload(args.payload_size, 9);

    let mut written = 0usize;
    let mut lsns = Vec::new();
    let mut last_lsn = None;

    while written < records {
        let n = args.batch_size.min(records - written);
        let batch = repeated_batch(n, payload.as_slice());
        let batch_lsns = wal.append_batch(&batch)?;

        last_lsn = batch_lsns.last().copied();
        if collect_lsns {
            lsns.extend(batch_lsns);
        }

        written += n;
    }

    wal.sync()?;
    let metrics = wal.metrics();
    drop(wal);

    Ok(WrittenWal {
        config,
        lsns,
        last_lsn,
        metrics,
    })
}

fn open_handle(
    config: WalConfig,
) -> BenchResult<(
    WalHandle<FsSegmentDirectory, ()>,
    wal::wal::report::RecoveryReport,
)> {
    let directory = FsSegmentDirectory::new(config.dir.clone());
    Ok(WalHandle::open(directory, config, ())?)
}

fn config_for(dir: &Path, payload_size: usize, target_segment_size: Option<u64>) -> WalConfig {
    let max_record_size = payload_size.max(1) as u32;
    let write_buffer_size = round_up(
        DEFAULT_WRITE_BUFFER_SIZE.max(RECORD_HEADER_LEN + payload_size.max(1)),
        4096,
    );

    let min_segment_size = min_valid_segment_size(payload_size.max(1), 0);
    let target_segment_size = target_segment_size
        .unwrap_or(64 * 1024 * 1024)
        .max(min_segment_size);

    WalConfig {
        dir: dir.to_path_buf(),
        identity: WalIdentity::new(0xA11CE, 1, 1),
        max_record_size,
        write_buffer_size,
        target_segment_size,
        sync_policy: SyncPolicy::OnExplicitSync,
        ..WalConfig::default()
    }
}

fn fresh_dir(args: &Args, name: &str) -> BenchResult<PathBuf> {
    let dir = args.dir.join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

fn repeated_batch<'a>(n: usize, payload: &'a [u8]) -> Vec<(RecordType, &'a [u8])> {
    (0..n)
        .map(|_| (RecordType::new(record_types::USER_MIN), payload))
        .collect()
}

fn make_payload(size: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![0u8; size];
    let mut x = seed ^ 0x9E37_79B9_7F4A_7C15;

    for b in &mut out {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        *b = (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 56) as u8;
    }

    out
}

fn corrupt_record_at(dir: &Path, lsn: Lsn, payload_size: usize) -> BenchResult<()> {
    let directory = FsSegmentDirectory::new(dir.to_path_buf());
    let metas = directory.list_segments()?;

    let meta = metas
        .iter()
        .filter(|meta| meta.base_lsn <= lsn)
        .last()
        .ok_or("could not find segment containing lsn")?;

    let logical_offset = lsn.as_u64() - meta.base_lsn.as_u64();
    let inside_record_offset = if payload_size > 0 {
        RECORD_HEADER_LEN as u64
    } else {
        0
    };

    let corrupt_offset = SEGMENT_HEADER_LEN + logical_offset + inside_record_offset;
    let mut bytes = fs::read(&meta.path)?;

    if corrupt_offset as usize >= bytes.len() {
        return Err(format!(
            "corrupt offset {} is outside {}",
            corrupt_offset,
            meta.path.display()
        )
        .into());
    }

    bytes[corrupt_offset as usize] ^= 0xFF;
    fs::write(&meta.path, bytes)?;
    Ok(())
}

fn small_segment_size(payload_size: usize, records_per_segment: u64) -> u64 {
    let record_len = physical_record_len(payload_size.max(1), 0);
    min_valid_segment_size(payload_size.max(1), 0) + record_len * records_per_segment
}

fn min_valid_segment_size(payload_size: usize, alignment: u32) -> u64 {
    let user_record = physical_record_len(payload_size, alignment);
    let seal_record = physical_record_len(24, alignment);
    SEGMENT_HEADER_LEN + user_record + seal_record
}

fn physical_record_len(payload_size: usize, alignment: u32) -> u64 {
    let logical = RECORD_HEADER_LEN + payload_size;
    if alignment == 0 {
        return logical as u64;
    }

    let alignment = alignment as usize;
    let rem = logical % alignment;
    if rem == 0 {
        logical as u64
    } else {
        (logical + (alignment - rem)) as u64
    }
}

fn row(
    name: &str,
    records: usize,
    payload_size: usize,
    duration: Duration,
    latencies_ns: Vec<u64>,
    metrics: &WalMetrics,
    extra: &str,
) -> BenchRow {
    let payload_bytes = records as u64 * payload_size as u64;
    let space_amp = if payload_bytes == 0 {
        0.0
    } else {
        metrics.current_wal_size as f64 / payload_bytes as f64
    };

    BenchRow {
        name: name.to_string(),
        records: records as u64,
        payload_bytes,
        wal_bytes: metrics.bytes_appended,
        duration,
        latencies_ns,
        notes: format!(
            "syncs={}, rollovers={}, wal_size={}, space_amp={:.2}, {}",
            metrics.sync_calls,
            metrics.segment_rollovers,
            metrics.current_wal_size,
            space_amp,
            extra,
        ),
    }
}

fn print_rows(rows: &[BenchRow]) {
    println!("## Results\n");
    println!(
        "| benchmark | records/ops | total | rec/s | payload MiB/s | WAL MiB/s | p50 | p95 | p99 | p99.9 | notes |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|");

    for row in rows {
        let secs = row.duration.as_secs_f64().max(0.000_000_001);
        let rec_s = row.records as f64 / secs;
        let payload_mib_s = row.payload_bytes as f64 / 1024.0 / 1024.0 / secs;
        let wal_mib_s = row.wal_bytes as f64 / 1024.0 / 1024.0 / secs;
        let stats = LatencyStats::from(&row.latencies_ns);

        println!(
            "| {} | {} | {} | {:.0} | {:.2} | {:.2} | {} | {} | {} | {} | {} |",
            row.name,
            row.records,
            fmt_duration(row.duration),
            rec_s,
            payload_mib_s,
            wal_mib_s,
            stats.p50,
            stats.p95,
            stats.p99,
            stats.p999,
            row.notes.replace('|', "/"),
        );
    }
}

struct LatencyStats {
    p50: String,
    p95: String,
    p99: String,
    p999: String,
}

impl LatencyStats {
    fn from(values: &[u64]) -> Self {
        if values.is_empty() {
            return Self {
                p50: "-".to_string(),
                p95: "-".to_string(),
                p99: "-".to_string(),
                p999: "-".to_string(),
            };
        }

        let mut sorted = values.to_vec();
        sorted.sort_unstable();

        Self {
            p50: fmt_ns(percentile(&sorted, 50.0)),
            p95: fmt_ns(percentile(&sorted, 95.0)),
            p99: fmt_ns(percentile(&sorted, 99.0)),
            p999: fmt_ns(percentile(&sorted, 99.9)),
        }
    }
}

fn percentile(sorted: &[u64], pct: f64) -> u64 {
    let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn fmt_duration(d: Duration) -> String {
    if d.as_secs() > 0 {
        format!("{:.3}s", d.as_secs_f64())
    } else {
        format!("{:.3}ms", ms(d))
    }
}

fn fmt_ns(ns: u64) -> String {
    if ns >= 1_000_000_000 {
        format!("{:.3}s", ns as f64 / 1_000_000_000.0)
    } else if ns >= 1_000_000 {
        format!("{:.3}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.3}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}

fn ns(d: Duration) -> u64 {
    d.as_nanos().min(u64::MAX as u128) as u64
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn round_up(value: usize, unit: usize) -> usize {
    if unit == 0 {
        return value;
    }

    let rem = value % unit;
    if rem == 0 {
        value
    } else {
        value + (unit - rem)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
