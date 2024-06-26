[![codecov][1]](https://codecov.io/gh/zoj613/zarr-ml)
[![CI][2]](https://github.com/zoj613/zarr-ml/actions/workflows/)
[![license][3]](https://github.com/zoj613/zarr-ml/blob/main/LICENSE)

# zarr-ml
An implementation of the Zarr version 3 specification.


## Usage Example
Below is a demonstration of the library's basic API.
### setup
```ocaml
open Zarr
open Zarr.Codecs
open Zarr.Storage

module Ndarray = Owl.Dense.Ndarray.Generic

let store =
  Result.get_ok @@
  FilesystemStore.open_or_create ~file_perm:0o777 "testdata.zarr";;
```
### create group
```ocaml
let group_node =
  Result.get_ok @@ Node.of_path "/some/group";;

FilesystemStore.create_group store group_node;;
```
### create an array
```ocaml
let array_node =
  Result.get_ok @@ Node.(group_node / "name");;

let codec_chain =
  {a2a = [Transpose [|2; 0; 1|]]
  ;a2b = Bytes Big
  ;b2b = [Gzip L2]};;

FilesystemStore.create_array
  ~codecs:codec_chain
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Bigarray.Float32 
  Float.neg_infinity
  array_node
  store;;
```
### open and write to an array
```ocaml
let slice = Owl_types.[|R [0; 20]; I 10; R []|];;
let x =
  Result.get_ok @@
  FilesystemStore.get_array array_node slice Bigarray.Float32 store;;

(* Do some computation on the array slice *)
let x' = Ndarray.map (fun _ -> Owl_stats_dist.uniform_rvs 0. 10.) x;;
FilesystemStore.set_array array_node slice x' store;;

FilesystemStore.get_array
  array_node
  Owl_types.[|R [0; 73]; L [10; 16]; R[0; 5]|]
  Bigarray.Float32
  store;;
(*           C0      C1       C2       C3      C4       C5 
 R[0,0]   6.106 4.76659   2.6251  5.76799 3.95144  1.95656 
 R[0,1]    -INF    -INF     -INF     -INF    -INF     -INF 
 R[1,0] 7.31409 6.64764 0.980762 0.530332 4.17086  5.45735 
 R[1,1]    -INF    -INF     -INF     -INF    -INF     -INF 
 R[2,0] 3.52729 5.15036 0.488728  4.40894 7.62077 0.891417 
            ...     ...      ...      ...     ...      ... 
R[71,1]    -INF    -INF     -INF     -INF    -INF     -INF 
R[72,0]    -INF    -INF     -INF     -INF    -INF     -INF 
R[72,1]    -INF    -INF     -INF     -INF    -INF     -INF 
R[73,0]    -INF    -INF     -INF     -INF    -INF     -INF 
R[73,1]    -INF    -INF     -INF     -INF    -INF     -INF *)
```
### create an array with sharding
```ocaml
let config =
  {chunk_shape = [|10; 3; 5|]
  ;codecs =
    {a2a = [Transpose [|2; 0; 1|]]
    ;a2b = Bytes Little
    ;b2b = [Gzip L5]}
  ;index_codecs =
    {a2a = []
    ;a2b = Bytes Big
    ;b2b = [Crc32c]}
  ;index_location = Start};;
let codec_chain =
  {a2a = []
  ;a2b = ShardingIndexed config
  ;b2b = [Crc32c]};;

let shard_node = Result.get_ok @@ Node.(group_node / "another");;

FilesystemStore.create_array
  ~codecs:codec_chain
  ~shape:[|100; 100; 50|]
  ~chunks:[|10; 15; 20|]
  Bigarray.Complex32
  Complex.zero
  shard_node
  store;;
```
### exploratory functions
```ocaml
FilesystemStore.find_all_nodes store |> List.map Node.to_path;;
(* - : string list = ["/"; "/some"; "/some/group/another"; "/some/group/name"; "/some/group"] *)

FilesystemStore.reshape store array_node [|25; 32; 10|];;

let meta =
  Result.get_ok @@
  FilesystemStore.group_metadata group_node store;;
GroupMetadata.show meta;; (* pretty prints the contents of the metadata *)

FilesystemStore.is_member store shard_node;;

FilesystemStore.find_child_nodes store group_node;;

FilesystemStore.erase_node store group_node;;
```

[1]: https://codecov.io/gh/zoj613/zarr-ml/graph/badge.svg?token=KOOG2Y1SH5
[2]: https://img.shields.io/github/actions/workflow/status/zoj613/zarr-ml/build-and-test.yml?branch=main
[3]: https://img.shields.io/github/license/zoj613/zarr-ml
