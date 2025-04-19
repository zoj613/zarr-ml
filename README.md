[![codecov][1]](https://codecov.io/gh/zoj613/zarr-ml)
[![CI][2]](https://github.com/zoj613/zarr-ml/actions/workflows/)
[![license][3]](https://github.com/zoj613/zarr-ml/blob/main/LICENSE)

# zarr-ml
This library provides an OCaml implementation of the Zarr version 3
storage format specification for chunked & compressed multi-dimensional
arrays, designed for use in parallel computing.

## Features
- Supports creating n-dimensional Zarr arrays and chunking them along any dimension.
- Compresses chunks using a variety of supported compression codecs.
- Supports indexing operations to read/write views of a Zarr array.
- Supports storing arrays in-memory, the local filesystem, or on an Amazon S3 bucket. It is also
  extensible, allowing users to easily create and use their own custom storage
  backends. See the example implementing an [In-memory Zip archive store][9] for more details.
- Supports both synchronous and asynchronous I/O via [Lwt][4] and [Eio][8]. The user can
  easily use their own scheduler of choice. See the [example][10] implementing
  a filesystem store that uses the [Picos][11] concurrency library for non-blocking I/O.
- Leverages the strong type system of Ocaml to create a type-safe API; making
  it impossible to create, read or write malformed arrays.
- Supports organizing arrays into heirarchies via groups.

## Documentation
API documentation can be found [here][5]. The full specification of the storage
format can be found [there][6].

## Installation
The library comes in several flavors dependending on the synchronous/asynchronous
backend of choice. To install the synchronous API, use
```shell
$ opam install zarr-sync
```
To install zarr with an asynchronous API powered by `Lwt` or `Eio`, use
```shell
$ opam install zarr-lwt
$ opam install zarr-eio
```
To install the development version using the latest git commit, do
```
# for zarr-sync
 opam pin add zarr-sync git+https://github.com/zoj613/zarr-ml 
# for zarr-lwt
 opam pin add zarr-lwt git+https://github.com/zoj613/zarr-ml 
# for zarr-eio
 opam pin add zarr-eio git+https://github.com/zoj613/zarr-ml 
 ```

## Quick start
Below is a demonstration of the library's API for synchronous reads/writes.
A similar example using the `Lwt`-backed Asynchronous API can be found [here][7]

```ocaml
open Zarr
open Zarr.Codecs
open Zarr.Indexing
open Zarr_sync.Storage
open IO.Syntax

let* store = FilesystemStore.create "testdata.zarr" in
(* create group *)
let* group_node = Node.Group.of_path "/some/group" in
let* () = FilesystemStore.Group.create store group_node in

(* creates an array with char data type and fill value '?' *)
let shape = [100; 100; 50] in
let chunks = [10; 15; 20] in
let codecs = [`Transpose [2; 0; 1]; `Bytes BE; `Gzip L2] in
let* array_node = Node.Array.(group_node / "name") in
let* () = FilesystemStore.Array.create ~codecs ~shape ~chunks Ndarray.Char '?' array_node store in

(* read/write from/to the array *)
let slice = [R (0, 20); I 10; F] in  (* same as [0:20, 10, :] in NumPy. *)
let* x = FilesystemStore.Array.read store array_node slice Ndarray.Char in
(* Do some computation on the array view *)
let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x in
let* () = FilesystemStore.Array.write store array_node slice x' in
let* y = FilesystemStore.Array.read store array_node slice Ndarray.Char in
assert (Ndarray.equal x' y);

(* creating an array with Sharding is supported. *)
let config =
  {chunk_shape = [5; 3; 5]
  ;codecs = [`Bytes LE; `Zstd (0, true)]
  ;index_codecs = [`Bytes BE; `Crc32c]
  ;index_location = Start} in
let codecs = [`ShardingIndexed config] in
let* shard_node = Node.Array.(group_node / "another") in
let* () = FilesystemStore.Array.create ~codecs ~shape ~chunks Ndarray.Complex32 Complex.zero shard_node store in

(* list all nodes inside a store and group them according to node type. *)
let* a, g = FilesystemStore.hierarchy store in
let array_paths = List.map Node.Array.to_path a in (*- : string list = ["/some/group/name"; "/some/group/another"] *)
let group_paths = List.map Node.Group.to_path g in (*- : string list = ["/"; "/some"; "/some/group"] *)
(* get child nodes of group_node .*)
let* a, g = FilesystemStore.Group.children store group_node in
(* resize an existing array. *)
let* () = FilesystemStore.Array.reshape store array_node [25; 32; 10] in
(* check if a node exists inside a store. *)
let* exists = FilesystemStore.Array.exists store shard_node in
(* get a metadata object that can be used to query a group/array's properties.
   See Metadata.Array & Metadata.Group modules *)
let* meta = FilesystemStore.Group.metadata store group_node in
(* get a prettified string of the contents of the metadata *)
print_endline @@ Metadata.Group.show meta;
(* give the specified node a new name *)
let* () = FilesystemStore.Array.rename store array_node "newarray" in
let* () = FilesystemStore.Group.rename store group_node "newgroup" in
(* delete the specified group node from store if it exists. *)
let group_node' = Result.get_ok (Node.Group.rename group_node "newgroup") in
let* () = FilesystemStore.Group.delete store group_node' in
(* wipe the store clean by deleting all nodes. *)
FilesystemStore.clear store
```

[1]: https://codecov.io/gh/zoj613/zarr-ml/graph/badge.svg?token=KOOG2Y1SH5
[2]: https://img.shields.io/github/actions/workflow/status/zoj613/zarr-ml/build-and-test.yml?branch=main
[3]: https://img.shields.io/github/license/zoj613/zarr-ml
[4]: https://ocsigen.org/lwt/latest/manual/manual
[5]: https://zoj613.github.io/zarr-ml
[6]: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html
[7]: https://zoj613.github.io/zarr-ml/zarr/Zarr/index.html#examples
[8]: https://github.com/ocaml-multicore/eio
[9]: https://github.com/zoj613/zarr-ml/tree/main/examples/zipstore.ml
[10]: https://github.com/zoj613/zarr-ml/tree/main/examples/picos_fs_store.ml
[11]: https://ocaml-multicore.github.io/picos/
