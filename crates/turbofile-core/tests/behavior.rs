use std::io;
use std::sync::mpsc;

use turbofile_core::{BackendKind, Dest, Driver, Op, OpenSpec, Payload, Reply};

fn submit_wait(driver: &Driver, op: Op) -> io::Result<Reply> {
    let (tx, rx) = mpsc::channel();
    driver.submit(
        op,
        Box::new(move |result| {
            tx.send(result).unwrap();
        }),
    );
    rx.recv().unwrap()
}

fn open_handle(driver: &Driver, path: &std::path::Path, spec: OpenSpec) -> u64 {
    match submit_wait(
        driver,
        Op::Open {
            path: path.to_path_buf(),
            spec,
        },
    )
    .unwrap()
    {
        Reply::Handle { id, .. } => id,
        other => panic!("expected handle, got {other:?}"),
    }
}

fn backends() -> Vec<BackendKind> {
    #[cfg(target_os = "macos")]
    return vec![BackendKind::Compio, BackendKind::DarwinAio];
    #[cfg(not(target_os = "macos"))]
    vec![BackendKind::Compio]
}

#[test]
fn append_writes_land_at_the_end() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("append.bin");
        std::fs::write(&path, b"prefix-").unwrap();
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            append: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);
        // pos 0 must be ignored for append handles.
        match submit_wait(
            &driver,
            Op::WriteAt {
                handle,
                pos: 0,
                data: Payload::Owned(b"suffix".to_vec()),
                append: true,
            },
        )
        .unwrap()
        {
            Reply::Written { n, end } => {
                assert_eq!(n, 6);
                assert_eq!(end, 13, "backend {kind:?}");
            }
            other => panic!("expected written, got {other:?}"),
        }
        submit_wait(&driver, Op::Close { handle }).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"prefix-suffix");
    }
}

#[test]
fn sixty_four_concurrent_reads_all_complete() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("many.bin");
        let payload: Vec<u8> = (0..256u32).flat_map(|i| i.to_le_bytes()).collect();
        std::fs::write(&path, &payload).unwrap();
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            read: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);

        let (tx, rx) = mpsc::channel();
        for i in 0..64u64 {
            let tx = tx.clone();
            let pos = (i % 16) * 64;
            driver.submit(
                Op::ReadAt {
                    handle,
                    pos,
                    dest: Dest::Alloc { len: 64 },
                },
                Box::new(move |result| {
                    tx.send((pos, result)).unwrap();
                }),
            );
        }
        drop(tx);
        let mut seen = 0;
        while let Ok((pos, result)) = rx.recv() {
            match result.unwrap() {
                Reply::Bytes(bytes) => {
                    assert_eq!(bytes, payload[pos as usize..pos as usize + 64]);
                }
                other => panic!("expected bytes, got {other:?}"),
            }
            seen += 1;
        }
        assert_eq!(seen, 64, "backend {kind:?}");
        submit_wait(&driver, Op::Close { handle }).unwrap();
    }
}

#[test]
fn sync_completes() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.bin");
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            write: true,
            create: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);
        submit_wait(
            &driver,
            Op::WriteAt {
                handle,
                pos: 0,
                data: Payload::Owned(b"durable".to_vec()),
                append: false,
            },
        )
        .unwrap();
        for data_only in [false, true] {
            match submit_wait(&driver, Op::Sync { handle, data_only }).unwrap() {
                Reply::Unit => {}
                other => panic!("expected unit, got {other:?}"),
            }
        }
        submit_wait(&driver, Op::Close { handle }).unwrap();
    }
}

#[test]
fn read_to_end_from_offset() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("toend.bin");
        std::fs::write(&path, b"0123456789").unwrap();
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            read: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);
        match submit_wait(&driver, Op::ReadToEnd { handle, pos: 4 }).unwrap() {
            Reply::Bytes(bytes) => assert_eq!(bytes, b"456789", "backend {kind:?}"),
            other => panic!("expected bytes, got {other:?}"),
        }
        match submit_wait(&driver, Op::ReadToEnd { handle, pos: 20 }).unwrap() {
            Reply::Bytes(bytes) => assert_eq!(bytes, b""),
            other => panic!("expected bytes, got {other:?}"),
        }
        submit_wait(&driver, Op::Close { handle }).unwrap();
    }
}

#[test]
fn whole_file_ops_roundtrip() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("whole.bin");
        let payload: Vec<u8> = (0..100_000u32).flat_map(|i| i.to_le_bytes()).collect();
        let driver = Driver::new(kind).unwrap();

        match submit_wait(
            &driver,
            Op::WriteFile {
                path: path.clone(),
                data: Payload::Owned(payload.clone()),
            },
        )
        .unwrap()
        {
            Reply::Written { n, .. } => assert_eq!(n, payload.len()),
            other => panic!("expected written, got {other:?}"),
        }
        match submit_wait(
            &driver,
            Op::ReadFile {
                path: path.clone(),
                inline_max: u64::MAX,
            },
        )
        .unwrap()
        {
            Reply::Bytes(bytes) => assert_eq!(bytes, payload, "backend {kind:?}"),
            other => panic!("expected bytes, got {other:?}"),
        }
    }
}

#[test]
fn missing_file_reports_not_found() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let driver = Driver::new(kind).unwrap();
        let err = submit_wait(
            &driver,
            Op::ReadFile {
                path: dir.path().join("nope.bin"),
                inline_max: u64::MAX,
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "backend {kind:?}");
    }
}

#[test]
fn ops_on_closed_handle_fail() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("closed.bin");
        std::fs::write(&path, b"x").unwrap();
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            read: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);
        submit_wait(&driver, Op::Close { handle }).unwrap();
        let err = submit_wait(
            &driver,
            Op::ReadAt {
                handle,
                pos: 0,
                dest: Dest::Alloc { len: 1 },
            },
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "backend {kind:?}");
    }
}

#[test]
fn zero_length_ops_complete() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero.bin");
        std::fs::write(&path, b"content").unwrap();
        let driver = Driver::new(kind).unwrap();

        let spec = OpenSpec {
            read: true,
            write: true,
            ..OpenSpec::default()
        };
        let handle = open_handle(&driver, &path, spec);
        match submit_wait(
            &driver,
            Op::ReadAt {
                handle,
                pos: 0,
                dest: Dest::Alloc { len: 0 },
            },
        )
        .unwrap()
        {
            Reply::Bytes(bytes) => assert_eq!(bytes, b""),
            other => panic!("expected bytes, got {other:?}"),
        }
        match submit_wait(
            &driver,
            Op::WriteAt {
                handle,
                pos: 3,
                data: Payload::Owned(Vec::new()),
                append: false,
            },
        )
        .unwrap()
        {
            Reply::Written { n, end } => {
                assert_eq!(n, 0);
                assert_eq!(end, 3, "backend {kind:?}");
            }
            other => panic!("expected written, got {other:?}"),
        }
        submit_wait(&driver, Op::Close { handle }).unwrap();
    }
}

#[test]
fn read_file_hands_off_files_above_inline_max() {
    for kind in backends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("handoff.bin");
        let payload: Vec<u8> = (0..16384u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &payload).unwrap();
        let driver = Driver::new(kind).unwrap();

        // At the threshold the whole read stays one submission.
        match submit_wait(
            &driver,
            Op::ReadFile {
                path: path.clone(),
                inline_max: payload.len() as u64,
            },
        )
        .unwrap()
        {
            Reply::Bytes(bytes) => assert_eq!(bytes, payload, "backend {kind:?}"),
            other => panic!("expected bytes, got {other:?}"),
        }

        // Above it the caller gets an open read handle to fill and close.
        let (handle, size) = match submit_wait(
            &driver,
            Op::ReadFile {
                path: path.clone(),
                inline_max: payload.len() as u64 - 1,
            },
        )
        .unwrap()
        {
            Reply::Handle { id, size, .. } => (id, size),
            other => panic!("expected handle, got {other:?} on {kind:?}"),
        };
        assert_eq!(size, payload.len() as u64);
        match submit_wait(
            &driver,
            Op::ReadAt {
                handle,
                pos: 0,
                dest: Dest::Alloc { len: payload.len() },
            },
        )
        .unwrap()
        {
            Reply::Bytes(bytes) => assert_eq!(bytes, payload, "backend {kind:?}"),
            other => panic!("expected bytes, got {other:?}"),
        }
        match submit_wait(&driver, Op::Close { handle }).unwrap() {
            Reply::Unit => {}
            other => panic!("expected unit, got {other:?}"),
        }
    }
}

/// A read to end whose first chunk reaches the size the file reported ends
/// there: one kernel round trip, and a buffer of exactly that size. Before,
/// the driver grew the buffer and read again just to see the zero.
#[cfg(target_os = "macos")]
#[test]
fn read_to_end_stops_at_the_reported_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exact.bin");
    let payload: Vec<u8> = (0..4096u32).map(|i| (i % 253) as u8).collect();
    std::fs::write(&path, &payload).unwrap();
    let driver = Driver::new(BackendKind::DarwinAio).unwrap();

    match submit_wait(
        &driver,
        Op::ReadFile {
            path: path.clone(),
            inline_max: u64::MAX,
        },
    )
    .unwrap()
    {
        Reply::Bytes(bytes) => {
            assert_eq!(bytes, payload);
            assert_eq!(bytes.capacity(), payload.len());
        }
        other => panic!("expected bytes, got {other:?}"),
    }

    let handle = open_handle(
        &driver,
        &path,
        OpenSpec {
            read: true,
            ..OpenSpec::default()
        },
    );
    match submit_wait(&driver, Op::ReadToEnd { handle, pos: 1000 }).unwrap() {
        Reply::Bytes(bytes) => {
            assert_eq!(bytes, payload[1000..]);
            assert_eq!(bytes.capacity(), payload.len() - 1000);
        }
        other => panic!("expected bytes, got {other:?}"),
    }
    submit_wait(&driver, Op::Close { handle }).unwrap();
}

/// A read submitted while one slow op holds `aio_suspend` is noticed within
/// the wait's cap, not a fixed millisecond. Each small read follows a 1 ms
/// lull, so the wait has backed off as far as it goes; at least one of three
/// must still settle well under a millisecond.
#[cfg(target_os = "macos")]
#[test]
fn a_read_beside_a_slow_op_is_noticed_within_the_wait_cap() {
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let big = dir.path().join("big.bin");
    let big_len = 128 << 20;
    std::fs::write(&big, vec![3u8; big_len]).unwrap();
    let small = dir.path().join("small.bin");
    std::fs::write(&small, vec![5u8; 4096]).unwrap();
    let driver = Driver::new(BackendKind::DarwinAio).unwrap();
    let handle = open_handle(
        &driver,
        &big,
        OpenSpec {
            read: true,
            ..OpenSpec::default()
        },
    );
    let small_read = || Op::ReadFile {
        path: small.clone(),
        inline_max: u64::MAX,
    };
    submit_wait(&driver, small_read()).unwrap();

    let (tx, rx) = mpsc::channel();
    driver.submit(
        Op::ReadAt {
            handle,
            pos: 0,
            dest: Dest::Alloc { len: big_len },
        },
        Box::new(move |result| tx.send(result).unwrap()),
    );
    let mut latencies = Vec::new();
    for _ in 0..3 {
        std::thread::sleep(Duration::from_millis(1));
        let started = Instant::now();
        match submit_wait(&driver, small_read()).unwrap() {
            Reply::Bytes(bytes) => assert_eq!(bytes.len(), 4096),
            other => panic!("expected bytes, got {other:?}"),
        }
        latencies.push(started.elapsed());
    }
    match rx.recv().unwrap().unwrap() {
        Reply::Bytes(bytes) => assert_eq!(bytes.len(), big_len),
        other => panic!("expected bytes, got {other:?}"),
    }
    let best = latencies.iter().min().unwrap();
    assert!(
        *best < Duration::from_micros(800),
        "small reads beside the slow op took {latencies:?}"
    );
    submit_wait(&driver, Op::Close { handle }).unwrap();
}

/// A FIFO with no writer blocks its reader's `open` until one appears, which
/// stands in for any open that stalls: a cold path, an endpoint-security scan.
/// Whole-file reads submitted around it must not wait behind it.
#[cfg(target_os = "macos")]
#[test]
fn opens_that_block_do_not_hold_up_other_reads() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("fifo");
    let cpath = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
    let payload = vec![7u8; 4096];
    let files: Vec<_> = (0..8)
        .map(|i| {
            let path = dir.path().join(format!("r{i}.bin"));
            std::fs::write(&path, &payload).unwrap();
            path
        })
        .collect();
    let driver = Driver::new(BackendKind::DarwinAio).unwrap();
    let read_file = |path: &std::path::Path| Op::ReadFile {
        path: path.to_path_buf(),
        inline_max: u64::MAX,
    };
    submit_wait(&driver, read_file(&files[0])).unwrap();
    std::thread::sleep(Duration::from_millis(10));

    let (tx, rx) = mpsc::channel();
    let (fifo_tx, fifo_rx) = mpsc::channel();
    let batch = files.iter().chain(files.iter());
    for (i, path) in batch.enumerate() {
        if i == files.len() {
            let fifo_tx = fifo_tx.clone();
            driver.submit(
                read_file(&fifo),
                Box::new(move |result| fifo_tx.send(result).unwrap()),
            );
        }
        let tx = tx.clone();
        driver.submit(
            read_file(path),
            Box::new(move |result| tx.send(result).unwrap()),
        );
    }

    let mut done = 0;
    while done < 2 * files.len() {
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Ok(Reply::Bytes(bytes))) => assert_eq!(bytes, payload),
            Ok(other) => panic!("expected bytes, got {other:?}"),
            Err(_) => {
                release_fifo(&fifo);
                panic!(
                    "{done} of {} regular reads completed while a FIFO open was blocking",
                    2 * files.len()
                );
            }
        }
        done += 1;
    }
    release_fifo(&fifo);
    drop(
        fifo_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the FIFO read settles once a writer closes"),
    );
}

/// Open the FIFO for writing on another thread (a writer's open blocks until a
/// reader has it open), write one byte and close so the reader sees EOF.
#[cfg(target_os = "macos")]
fn release_fifo(fifo: &std::path::Path) {
    let fifo = fifo.to_path_buf();
    std::thread::spawn(move || {
        use std::io::Write;
        let mut writer = std::fs::OpenOptions::new().write(true).open(fifo).unwrap();
        writer.write_all(b"x").unwrap();
    });
}

/// Under a backlog the opens run on helper threads and their descriptors and
/// errors come back to the driver as messages. Every kind of whole-file op
/// submitted in one burst must still settle the way it does alone.
#[cfg(target_os = "macos")]
#[test]
fn a_burst_of_whole_file_ops_settles_each_one() {
    let dir = tempfile::tempdir().unwrap();
    let small = vec![9u8; 2048];
    let large = vec![4u8; 1 << 16];
    let small_paths: Vec<_> = (0..24)
        .map(|i| {
            let path = dir.path().join(format!("s{i}.bin"));
            std::fs::write(&path, &small).unwrap();
            path
        })
        .collect();
    let large_path = dir.path().join("large.bin");
    std::fs::write(&large_path, &large).unwrap();
    let missing = dir.path().join("absent.bin");
    let written = dir.path().join("written.bin");
    let driver = Driver::new(BackendKind::DarwinAio).unwrap();

    let (tx, rx) = mpsc::channel();
    let submit = |tag: &'static str, op: Op| {
        let tx = tx.clone();
        driver.submit(op, Box::new(move |result| tx.send((tag, result)).unwrap()));
    };
    for (i, path) in small_paths.iter().enumerate() {
        submit(
            "small",
            Op::ReadFile {
                path: path.clone(),
                inline_max: u64::MAX,
            },
        );
        match i {
            6 => submit(
                "missing",
                Op::ReadFile {
                    path: missing.clone(),
                    inline_max: u64::MAX,
                },
            ),
            12 => submit(
                "handle",
                Op::ReadFile {
                    path: large_path.clone(),
                    inline_max: 4096,
                },
            ),
            18 => submit(
                "written",
                Op::WriteFile {
                    path: written.clone(),
                    data: Payload::Owned(large.clone()),
                },
            ),
            _ => {}
        }
    }
    drop(tx);

    let mut smalls = 0;
    let mut seen = Vec::new();
    for (tag, result) in rx {
        match (tag, result) {
            ("small", Ok(Reply::Bytes(bytes))) => {
                assert_eq!(bytes, small);
                smalls += 1;
            }
            ("missing", Err(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
                seen.push(tag);
            }
            ("handle", Ok(Reply::Handle { id, size, .. })) => {
                assert_eq!(size, large.len() as u64);
                match submit_wait(&driver, Op::ReadToEnd { handle: id, pos: 0 }).unwrap() {
                    Reply::Bytes(bytes) => assert_eq!(bytes, large),
                    other => panic!("expected bytes, got {other:?}"),
                }
                submit_wait(&driver, Op::Close { handle: id }).unwrap();
                seen.push(tag);
            }
            ("written", Ok(Reply::Written { n, end })) => {
                assert_eq!((n, end), (large.len(), large.len() as u64));
                assert_eq!(std::fs::read(&written).unwrap(), large);
                seen.push(tag);
            }
            (tag, other) => panic!("{tag}: unexpected result {other:?}"),
        }
    }
    assert_eq!(smalls, small_paths.len());
    seen.sort();
    assert_eq!(seen, ["handle", "missing", "written"]);
}

/// An op whose open is still out at a helper thread has no queued job and no
/// aiocb for a cancel to find; the cancel must still win when the open comes
/// back.
#[cfg(target_os = "macos")]
#[test]
fn a_cancel_during_a_blocked_open_settles_cancelled() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("fifo");
    let cpath = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) }, 0);
    let files: Vec<_> = (0..8)
        .map(|i| {
            let path = dir.path().join(format!("r{i}.bin"));
            std::fs::write(&path, vec![1u8; 512]).unwrap();
            path
        })
        .collect();
    let driver = Driver::new(BackendKind::DarwinAio).unwrap();
    let read_file = |path: &std::path::Path| Op::ReadFile {
        path: path.to_path_buf(),
        inline_max: u64::MAX,
    };
    submit_wait(&driver, read_file(&files[0])).unwrap();
    std::thread::sleep(Duration::from_millis(10));

    let (tx, rx) = mpsc::channel();
    for path in &files {
        let tx = tx.clone();
        driver.submit(
            read_file(path),
            Box::new(move |result| tx.send(result).unwrap()),
        );
    }
    let (fifo_tx, fifo_rx) = mpsc::channel();
    let fifo_id = driver.submit(
        read_file(&fifo),
        Box::new(move |result| fifo_tx.send(result).unwrap()),
    );
    for _ in &files {
        rx.recv_timeout(Duration::from_secs(2))
            .expect("regular reads complete beside the blocked open")
            .unwrap();
    }

    driver.cancel(fifo_id);
    std::thread::sleep(Duration::from_millis(20));
    release_fifo(&fifo);
    let result = fifo_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the FIFO op settles once a writer closes");
    match result {
        Err(e) if turbofile_core::is_cancelled(&e) => {}
        other => panic!("expected ECANCELED, got {other:?}"),
    }
}
