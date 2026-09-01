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

#[test]
fn compio_write_read_roundtrip() {
    write_read_roundtrip(BackendKind::Compio);
}

#[cfg(target_os = "macos")]
#[test]
fn darwin_aio_write_read_roundtrip() {
    write_read_roundtrip(BackendKind::DarwinAio);
}

fn write_read_roundtrip(kind: BackendKind) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roundtrip.bin");
    let driver = Driver::new(kind).unwrap();

    let spec = OpenSpec {
        write: true,
        create: true,
        truncate: true,
        ..OpenSpec::default()
    };
    let handle = open_handle(&driver, &path, spec);

    let payload = b"turbofile speaks completion queues".to_vec();
    match submit_wait(
        &driver,
        Op::WriteAt {
            handle,
            pos: 0,
            data: Payload::Owned(payload.clone()),
            append: false,
        },
    )
    .unwrap()
    {
        Reply::Written { n, end } => {
            assert_eq!(n, payload.len());
            assert_eq!(end, payload.len() as u64);
        }
        other => panic!("expected written, got {other:?}"),
    }
    submit_wait(&driver, Op::Close { handle }).unwrap();

    let spec = OpenSpec {
        read: true,
        ..OpenSpec::default()
    };
    let handle = open_handle(&driver, &path, spec);
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
        Reply::Bytes(bytes) => assert_eq!(bytes, payload),
        other => panic!("expected bytes, got {other:?}"),
    }
    submit_wait(&driver, Op::Close { handle }).unwrap();
}

#[test]
fn compio_borrowed_write_and_read_into() {
    borrowed_write_and_read_into(BackendKind::Compio);
}

#[cfg(target_os = "macos")]
#[test]
fn darwin_aio_borrowed_write_and_read_into() {
    borrowed_write_and_read_into(BackendKind::DarwinAio);
}

fn borrowed_write_and_read_into(kind: BackendKind) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero_copy.bin");
    let driver = Driver::new(kind).unwrap();

    let spec = OpenSpec {
        read: true,
        write: true,
        create: true,
        truncate: true,
        ..OpenSpec::default()
    };
    let handle = open_handle(&driver, &path, spec);

    let source: Box<[u8]> = b"zero copy or bust".to_vec().into_boxed_slice();
    match submit_wait(
        &driver,
        Op::WriteAt {
            handle,
            pos: 0,
            data: Payload::Borrowed {
                ptr: source.as_ptr(),
                len: source.len(),
            },
            append: false,
        },
    )
    .unwrap()
    {
        Reply::Written { n, .. } => assert_eq!(n, source.len()),
        other => panic!("expected written, got {other:?}"),
    }

    let mut sink: Box<[u8]> = vec![0u8; source.len()].into_boxed_slice();
    match submit_wait(
        &driver,
        Op::ReadAt {
            handle,
            pos: 0,
            dest: Dest::Into {
                ptr: sink.as_mut_ptr(),
                len: sink.len(),
            },
        },
    )
    .unwrap()
    {
        Reply::Read { n } => assert_eq!(n, source.len()),
        other => panic!("expected read, got {other:?}"),
    }
    assert_eq!(&sink[..], &source[..]);

    // Reading past EOF into caller memory reports the short length.
    match submit_wait(
        &driver,
        Op::ReadAt {
            handle,
            pos: 8,
            dest: Dest::Into {
                ptr: sink.as_mut_ptr(),
                len: sink.len(),
            },
        },
    )
    .unwrap()
    {
        Reply::Read { n } => assert_eq!(n, source.len() - 8),
        other => panic!("expected read, got {other:?}"),
    }
    assert_eq!(&sink[..source.len() - 8], &source[8..]);

    submit_wait(&driver, Op::Close { handle }).unwrap();
}

#[test]
fn backend_label_reports_the_live_driver() {
    let driver = Driver::new(BackendKind::Compio).unwrap();
    let label = driver.backend_label();
    assert!(label.starts_with("compio-"), "got {label}");
    if std::env::var("TURBOFILE_EXPECT_IOURING").is_ok() {
        assert_eq!(label, "compio-io-uring");
    }
    #[cfg(target_os = "macos")]
    {
        assert_eq!(label, "compio-polling");
        let aio = Driver::new(BackendKind::DarwinAio).unwrap();
        assert_eq!(aio.backend_label(), "darwin-aio");
    }
}
