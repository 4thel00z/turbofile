# Changelog

## [0.1.4](https://github.com/4thel00z/turbofile/compare/v0.1.3...v0.1.4) (2026-09-08)


### Bug Fixes

* **darwin:** a message taken while idle counts toward the next wait ([da733ae](https://github.com/4thel00z/turbofile/commit/da733ae998fe81ab075bec938763ab549dd4c80c))
* **darwin:** tell the two channels apart when the idle wait ends ([305a748](https://github.com/4thel00z/turbofile/commit/305a74837b6ce377068fc09ea3bae8f086b9baa2))


### Performance Improvements

* **darwin:** adaptive aio_suspend wait, 20 us to 160 us ([5531d31](https://github.com/4thel00z/turbofile/commit/5531d318ef824b7612c3c0011fef4beaa384039e))
* **darwin:** adaptive aio_suspend wait, 20 us to 160 us ([2003152](https://github.com/4thel00z/turbofile/commit/2003152b2b0f46ecd8bf30258d6b54d074f88b8b))
* **darwin:** end a read-to-end where fstat says the file ends ([51c32e0](https://github.com/4thel00z/turbofile/commit/51c32e04d099aa9320c968898b010f541993beda))
* **darwin:** one AIO round trip per read-to-end ([b963ad8](https://github.com/4thel00z/turbofile/commit/b963ad84ada71d21014cf14110cb5fc562b5235f))
* **darwin:** open on helper threads while the driver has a backlog ([b0bd2aa](https://github.com/4thel00z/turbofile/commit/b0bd2aa5469bbcb0823d6adb2fa105b0a7e4dc26))
* **darwin:** open on helper threads while the driver has a backlog ([8323a76](https://github.com/4thel00z/turbofile/commit/8323a76c760b04f81df5f825787b771ecb0661d4))


### Documentation

* keep turbofile.dev's bench table in step with the README ([df3f1a2](https://github.com/4thel00z/turbofile/commit/df3f1a21476a38f7b97fcd4d59e8e23f730990b4))
* keep turbofile.dev's bench table in step with the README ([814cdca](https://github.com/4thel00z/turbofile/commit/814cdcae98c12f184323bb03f4cb1a8103d9c35d))
* one round trip per read-to-end, and the driver-thread read that did not pay ([e397eb6](https://github.com/4thel00z/turbofile/commit/e397eb6997d3c26efc7ab2c9f6907e63da294bce))
* **perf:** the notice cliff, the waits measured, and the one that shipped ([cc39f4e](https://github.com/4thel00z/turbofile/commit/cc39f4e344e83643beac34fa1f5041972fed4490))
* **perf:** the storm split, and opens off the driver thread ([a6b9123](https://github.com/4thel00z/turbofile/commit/a6b9123dd18e52ae4a0ac2457b8339cbca132ee7))

## [0.1.3](https://github.com/4thel00z/turbofile/compare/v0.1.2...v0.1.3) (2026-09-03)


### Bug Fixes

* **bridge:** retry the doorbell pipe syscalls on EINTR ([bf29a6d](https://github.com/4thel00z/turbofile/commit/bf29a6d1accce7234fcdecf4d51e95678b992498))
* **core:** cancelling an await aborts the kernel op ([#2](https://github.com/4thel00z/turbofile/issues/2)) ([cfeb92e](https://github.com/4thel00z/turbofile/commit/cfeb92e346f15b51d1b12f4613d20bc49a666871))


### Performance Improvements

* **bridge:** wake the loop through a pipe, not call_soon_threadsafe ([041629e](https://github.com/4thel00z/turbofile/commit/041629eb658d959c0e1cbef56a8f3e2c50248b8f))
* pipe doorbell and 512 KiB parallel read chunks ([eb810a8](https://github.com/4thel00z/turbofile/commit/eb810a88cb6d9b8471795e024ce3acb2c7d0c4bd))
* **read:** 512 KiB parallel chunks and inline size checks for large reads ([4d67a26](https://github.com/4thel00z/turbofile/commit/4d67a26083da612c4d193229ae036600bd2d6be3))
* **read:** read_bytes hands large files to the parallel zero-copy fill ([7e3ca05](https://github.com/4thel00z/turbofile/commit/7e3ca05c3cf7a87411aa807e620d12428d32cb2f))
* **read:** read_bytes hands large files to the parallel zero-copy fill ([47c1678](https://github.com/4thel00z/turbofile/commit/47c1678c0662368c85b37424240574f69679cb5d))

## [0.1.2](https://github.com/4thel00z/turbofile/compare/v0.1.1...v0.1.2) (2026-09-01)


### Documentation

* state current benchmark numbers only; bench an open-file read ([218fd5a](https://github.com/4thel00z/turbofile/commit/218fd5a3731944225477dcf51b2ec1dd487239d8))

## [0.1.1](https://github.com/4thel00z/turbofile/compare/v0.1.0...v0.1.1) (2026-09-01)


### Bug Fixes

* make the fast path decline explicitly instead of leaning on EINVAL ([cdaf880](https://github.com/4thel00z/turbofile/commit/cdaf880e180eb29977f556169719db21dcccf425))
* report filesystem as "?" where /proc is unavailable ([75962ea](https://github.com/4thel00z/turbofile/commit/75962ea0fa7f8c3cf3df1b7e542543de50738653))


### Performance Improvements

* serve page-cache-hot reads inline on macOS with mincore + pread ([3272a35](https://github.com/4thel00z/turbofile/commit/3272a35a136f15083bcf8ac388123fb960e1eac3))
* serve page-cache-hot reads inline with preadv2(RWF_NOWAIT) ([9ca717c](https://github.com/4thel00z/turbofile/commit/9ca717c5e6bd93006430564953cf7f2c89f2dfc0))

## [0.1.0](https://github.com/4thel00z/turbofile/compare/v0.1.0...v0.1.0) (2026-09-01)


### Features

* **bench:** benchmark suite against aiofiles ([4ac478e](https://github.com/4thel00z/turbofile/commit/4ac478ee2ae855004e4ed3dab311f9c270a5796e))
* **core:** backend drivers with zero-copy payloads ([1500fb4](https://github.com/4thel00z/turbofile/commit/1500fb43e0d0f08e221144febb0990817cfa97ec))
* **py:** asyncio bridge with completion batching ([7748301](https://github.com/4thel00z/turbofile/commit/7748301bca2c0611b0e094a3f7e708a311dd77f0))
* **python:** aiofiles-compatible API and whole-file helpers ([9280582](https://github.com/4thel00z/turbofile/commit/928058288a9115e7f66315969587dcd57a0fcd46))


### Continuous Integration

* test workflow and release-please publishing ([f52c461](https://github.com/4thel00z/turbofile/commit/f52c461f5207c5d3989d9fa41a315b6f61347d51))
