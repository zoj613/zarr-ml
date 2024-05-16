# zarr-ml
An implementation of the Zarr version 3 specification.


## Example
```ocaml
open Zarr
open Zarr.Codecs
open Zarr.Storage
open Zarr.Metadata
open Zarr.Extensions
module Ndarray = Owl.Dense.Ndarray.Generic

let store =
  Result.get_ok @@
  FilesystemStore.open_or_create ~file_perm:0o777 "testdata.zarr";;

let group_node =
  Result.get_ok @@ Node.of_path "/some/group";;

FilesystemStore.create_group store group_node;;

let array_node =
  Result.get_ok @@ Node.(group_node / "name");;

let shard_config = {
  chunk_shape = [|5; 5; 10|];
  codecs = Chain.create [] (Bytes Little) [Gzip L5];
  index_codecs = Chain.create [] (Bytes Big) [Crc32c];
  index_location = Start
};;
let codec_chain =
  Chain.create [Transpose [|0; 1; 2|]] (ShardingIndexed shard_config) [];;

FilesystemStore.create_array
  ~codecs:codec_chain
  ~shape:[|100; 100; 50|]
  ~chunks:[|15; 15; 20|]
  (FillValue.Float Float.neg_infinity)
  Datatype.Float32
  array_node
  store;;

FilesystemStore.find_all_nodes store |> List.map Node.to_path;;
(* - : string list = ["/"; "/some"; "/some/group/name"; "/some/group"] *)

let slice = Owl_types.[|R [0; 20]; I 10; R []|];;
let x =
  Result.get_ok @@
  FilesystemStore.get_array array_node slice Bigarray.Float32 store;;
(*
          C0   C1   C2   C3   C4      C45  C46  C47  C48  C49 
 R[0,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
 R[1,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
 R[2,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
 R[3,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
 R[4,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
         ...  ...  ...  ...  ... ...  ...  ...  ...  ...  ... 
R[16,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
R[17,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
R[18,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
R[19,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF 
R[20,0] -INF -INF -INF -INF -INF ... -INF -INF -INF -INF -INF *)

(* Do some computation on the array slice *)
let x' = Ndarray.map (fun _ -> Owl_stats_dist.uniform_rvs 0. 100.) x;;
FilesystemStore.set_array array_node slice x' store;;

FilesystemStore.get_array
  array_node
  Owl_types.[|R [0; 73]; L [10; 16]; R[0; 5]|]
  Bigarray.Float32
  store;;
(*
             C0      C1      C2      C3      C4      C5 
 R[0,0] 68.0272  44.914 85.2431 39.0772  26.582  16.577 
 R[0,1]    -INF    -INF    -INF    -INF    -INF    -INF 
 R[1,0]  88.418 77.0368 43.4968 45.1263 8.95641 76.9155 
 R[1,1]    -INF    -INF    -INF    -INF    -INF    -INF 
 R[2,0] 98.4036 77.8744 67.6689 56.8803 37.0718  97.042 
            ...     ...     ...     ...     ...     ... 
R[71,1]    -INF    -INF    -INF    -INF    -INF    -INF 
R[72,0]    -INF    -INF    -INF    -INF    -INF    -INF 
R[72,1]    -INF    -INF    -INF    -INF    -INF    -INF 
R[73,0]    -INF    -INF    -INF    -INF    -INF    -INF 
R[73,1]    -INF    -INF    -INF    -INF    -INF    -INF *)

FilesystemStore.reshape store array_node [|25; 32; 10|];;
FilesystemStore.get_array
  array_node
  Owl_types.[|R []; I 10; R[0; 5]|]
  Bigarray.Float32
  store;;
(*
             C0      C1      C2      C3      C4      C5 
 R[0,0] 68.0272  44.914 85.2431 39.0772  26.582  16.577 
 R[1,0]  88.418 77.0368 43.4968 45.1263 8.95641 76.9155 
 R[2,0] 98.4036 77.8744 67.6689 56.8803 37.0718  97.042 
 R[3,0] 22.8653 20.1767 88.9549 22.1052 9.86822 10.8826 
 R[4,0] 55.6043 93.8599 60.3723  40.543 46.8199  97.282 
            ...     ...     ...     ...     ...     ... 
R[20,0] 61.2473 78.8035 52.3056 59.5631 78.2462 52.4205 
R[21,0]    -INF    -INF    -INF    -INF    -INF    -INF 
R[22,0]    -INF    -INF    -INF    -INF    -INF    -INF 
R[23,0]    -INF    -INF    -INF    -INF    -INF    -INF 
R[24,0]    -INF    -INF    -INF    -INF    -INF    -INF *)

FilesystemStore.array_metadata array_node store
|> Result.get_ok
|> ArrayMetadata.shape;;
(* - : int array = [|25; 32; 10|] *)
```
