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
- Supports storing arrays in-memory or the local filesystem. It is also
  extensible, allowing users to easily create and use their own custom storage
  backends. See the example implementing a [Zip file store][9] for more details.
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
### setup
```ocaml
open Zarr
open Zarr.Codecs
open Zarr.Indexing
open Zarr_sync.Storage
open Deferred.Infix  (* opens infix operators >>= and >>| for monadic bind & map *)

let store = FilesystemStore.create "testdata.zarr";;
```
### create group
```ocaml
let group_node = Node.Group.of_path "/some/group";;
FilesystemStore.Group.create store group_node;;
```
### create an array
```ocaml
let array_node = Node.Array.(group_node / "name");;
(* creates an array with char data type and fill value '?' *)
FilesystemStore.Array.create
  ~codecs:[`Transpose [|2; 0; 1|]; `Bytes BE; `Gzip L2]
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Ndarray.Char 
  '?'
  array_node
  store;;
```
### read/write from/to an array
```ocaml
let slice = [|R [|0; 20|]; I 10; R [||]|];;
let x = FilesystemStore.Array.read store array_node slice Ndarray.Char;;
(* Do some computation on the array slice *)
let x' = Zarr.Ndarray.map (fun _ -> Random.int 256 |> Char.chr) x;;
FilesystemStore.Array.write store array_node slice x';;
let y = FilesystemStore.Array.read store array_node slice Ndarray.Char;;
assert (Ndarray.equal x' y);;
```
### create an array with sharding
```ocaml
let config =
  {chunk_shape = [|5; 3; 5|]
  ;codecs = [`Transpose [|2; 0; 1|]; `Bytes LE; `Zstd (0, true)]
  ;index_codecs = [`Bytes BE; `Crc32c]
  ;index_location = Start};;

let shard_node = Node.Array.(group_node / "another");;

FilesystemStore.Array.create
  ~codecs:[`ShardingIndexed config]
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Ndarray.Complex32
  Complex.zero
  shard_node
  store;;
```
### exploratory functions
```ocaml
let a, g = FilesystemStore.hierarchy store;;
List.map Node.Array.to_path a;;
(*- : string list = ["/some/group/name"; "/some/group/another"] *)
List.map Node.Group.to_path g;;
(*- : string list = ["/"; "/some"; "/some/group"] *)

FilesystemStore.Array.reshape store array_node [|25; 32; 10|];;

let meta = FilesystemStore.Group.metadata store group_node;;
Metadata.Group.show meta;; (* pretty prints the contents of the metadata *)

FilesystemStore.Array.exists store shard_node;;
FilesystemStore.Group.exists store group_node;;

let a, g = FilesystemStore.Group.children store group_node;;
List.map Node.Array.to_path a;;
(*- : string list = ["/some/group/name"; "/some/group/another"] *)
List.map Node.Group.to_path g;;
(*- : string list = [] *)

FilesystemStore.Group.delete store group_node;;
FilesystemStore.clear store;; (* clears the store *)
FilesystemStore.Group.rename store group_node "new_name";;
FilesystemStore.Array.rename store anode "new_name";;
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
