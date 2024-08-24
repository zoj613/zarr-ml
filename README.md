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
  extensible, allowing users to create and use their own custom storage backends.
- Supports both synchronous and concurrent I/O via [Lwt][4] and [Eio][8].
- Leverages the strong type system of Ocaml to create a type-safe API; making
  it impossible to create, read or write malformed arrays.
- Supports organizing arrays into heirarchies via groups.

## Documentation
API documentation can be found [here][5]. The full specification of the storage
format can be found [there][6].

## Quick start
Below is a demonstration of the library's API for synchronous reads/writes.
A similar example using the `Lwt`-backed Asynchronous API can be found [here][7]
### setup
```ocaml
open Zarr.Metadata
open Zarr.Node
open Zarr.Codecs
open Zarr_sync.Storage

(* opens infix operators >>= and >>| for monadic bind & map *)
open FilesytemStore.Deferred.Infix

let store = FilesystemStore.create_store "testdata.zarr";;
```
### create group
```ocaml
let group_node = GroupNode.of_path "/some/group";;
FilesystemStore.create_group store group_node;;
```
### create an array
```ocaml
let array_node = ArrayNode.(group_node / "name");;
(* creates an array with char data type and fill value '?' *)
FilesystemStore.create_array
  ~codecs:[`Transpose [|2; 0; 1|]; `Bytes BE; `Gzip L2]
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Bigarray.Char 
  '?'
  array_node
  store;;
```
### read/write from/to an array
```ocaml
let slice = Owl_types.[|R [0; 20]; I 10; R []|];;
let x = FilesystemStore.read_array store array_node slice Bigarray.Char;;
(* Do some computation on the array slice *)
let x' =
  Owl.Dense.Ndarray.Generic.map
    (fun _ -> Owl_stats_dist.uniform_int_rvs ~a:0 ~b:255 |> Char.chr) x;;
FilesystemStore.write_array store array_node slice x';;

FilesystemStore.read_array
  store array_node Owl_types.[|R [0; 73]; I 10; R [0; 5]|] Bigarray.Char;;
(*       C0  C1  C2  C3  C4  C5 
 R[0,0]   =      Ð   �       
 R[1,0]   d   ®   ê   ~   1    
 R[2,0]      £      Q   Ò   ø 
 R[3,0]   e   ^      Ò   Ê   B 
 R[4,0]   ú   2   �   1   `   n 
        ... ... ... ... ... ... 
R[69,0]   ?   ?   ?   ?   ?   ? 
R[70,0]   ?   ?   ?   ?   ?   ? 
R[71,0]   ?   ?   ?   ?   ?   ? 
R[72,0]   ?   ?   ?   ?   ?   ? 
R[73,0]   ?   ?   ?   ?   ?   ?  *)
```
### create an array with sharding
```ocaml
let config =
  {chunk_shape = [|5; 3; 5|]
  ;codecs = [`Transpose [|2; 0; 1|]; `Bytes LE; `Gzip L5]
  ;index_codecs = [`Bytes BE; `Crc32c]
  ;index_location = Start};;

let shard_node = ArrayNode.(group_node / "another");;

FilesystemStore.create_array
  ~codecs:[`ShardingIndexed config]
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Bigarray.Complex32
  Complex.zero
  shard_node
  store;;
```
### exploratory functions
```ocaml
let a, g = FilesystemStore.find_all_nodes store;;
List.map ArrayNode.to_path a;;
(*- : string list = ["/some/group/name"; "/some/group/another"] *)
List.map GroupNode.to_path g;;
(*- : string list = ["/"; "/some"; "/some/group"] *)

FilesystemStore.reshape store array_node [|25; 32; 10|];;

let meta = FilesystemStore.group_metadata store group_node;;
GroupMetadata.show meta;; (* pretty prints the contents of the metadata *)

FilesystemStore.array_exists store shard_node;;
FilesystemStore.group_exists store group_node;;

let a, g = FilesystemStore.find_child_nodes store group_node;;
List.map ArrayNode.to_path a;;
(*- : string list = ["/some/group/name"; "/some/group/another"] *)
List.map GroupNode.to_path g;;
(*- : string list = [] *)

FilesystemStore.erase_group_node store group_node;;
```

[1]: https://codecov.io/gh/zoj613/zarr-ml/graph/badge.svg?token=KOOG2Y1SH5
[2]: https://img.shields.io/github/actions/workflow/status/zoj613/zarr-ml/build-and-test.yml?branch=main
[3]: https://img.shields.io/github/license/zoj613/zarr-ml
[4]: https://ocsigen.org/lwt/latest/manual/manual
[5]: https://zoj613.github.io/zarr-ml
[6]: https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html
[7]: https://zoj613.github.io/zarr-ml/zarr/Zarr/index.html#examples
[8]: https://github.com/ocaml-multicore/eio
