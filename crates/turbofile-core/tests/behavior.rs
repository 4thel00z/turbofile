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
