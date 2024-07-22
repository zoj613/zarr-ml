(* Copyright (c) 2024, Zolisa Bleki

  SPDX-License-Identifier: BSD-3-Clause *)

(**
   [zarr] Provides an Ocaml implementation of the Zarr version 3 storage
   format specification. It supports creation of arrays and groups as well
   as chunking arrays along any dimension. One can store a Zarr hierarchy in
   memory or on disk. Zarr also supports reading zarr hierarchies created using
   other implementations, as long as they are spec-compliant.

   Consult the {{!examples}examples} and {{!limitations}limitations} for more info.

   {3 References}
   {ul
    {- {{:https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html}The Zarr Version 3 specification.}} 
    {- {{:https://zarr.dev/}Zarr community site.}}
   }
   *)

(** {1 Node} *)

module Node = Node

(** {1 Metadata} *)

module ArrayMetadata = Metadata.ArrayMetadata
module GroupMetadata = Metadata.GroupMetadata

(** {1 Storage} *)

module Storage = Storage

(** {1 Codecs} *)

module Codecs = Codecs

(** {1 Indexing} *)

module Indexing = Util.Indexing

(** {1:examples Examples}
    
    {2:create_array Create, read & write array.}
    {@ocaml[
    open Zarr
    open Zarr.Node
    open Zarr.Codecs
    open Zarr.Storage

    let store =
      Result.get_ok @@ FilesystemStore.open_or_create "testdata.zarr" in
    let group_node = Result.get_ok @@ GroupNode.of_path "/some/group" in
    FilesystemStore.create_group store group_node;
    let array_node = Result.get_ok @@ ArrayNode.(group_node / "name") in
    FilesystemStore.create_array
      ~codecs:[`Transpose [|2; 0; 1|]; `Bytes BE; `Gzip L2]
      ~shape:[|100; 100; 50|]
      ~chunks:[|10; 15; 20|]
      Bigarray.Float32 
      Float.neg_infinity
      array_node
      store;
    let slice = Owl_types.[|R [0; 20]; I 10; R []|] in
    let x =
      Result.get_ok @@
      FilesystemStore.get_array store array_node slice Bigarray.Float32 in
    let x' =
      Owl.Dense.Ndarray.Generic.map
        (fun _ -> Owl_stats_dist.uniform_rvs 0. 10.) x in
    FilesystemStore.set_array store array_node slice x';
    ]}

    {2:sharding Using sharding codec.}
    {@ocaml[
    let config =
      {chunk_shape = [|5; 3; 5|]
      ;codecs = [`Transpose [|2; 0; 1|]; `Bytes LE; `Gzip L5]
      ;index_codecs = [`Bytes BE; `Crc32c]
      ;index_location = Start} in
    let shard_node = Result.get_ok @@ ArrayNode.(group_node / "another") in
    FilesystemStore.create_array
      ~codecs:[`ShardingIndexed config]
      ~shape:[|100; 100; 50|]
      ~chunks:[|10; 15; 20|]
      Bigarray.Complex32
      Complex.zero
      shard_node
      store;
    ]}

    {2:explore Explore a Zarr hierarchy.}
    Functions to query a zarr hierarchy are provided. These include listing
    all nodes, finding children of a group node, resizing an array, deleting
    nodes, obtaining metadata of a node, and more.
    {@ocaml[
    let a, g = FilesystemStore.find_all_nodes store in
    FilesystemStore.reshape store array_node [|25; 32; 10|];
    let meta =
      Result.get_ok @@ FilesystemStore.group_metadata store group_node in
    GroupMetadata.show meta;
    FilesystemStore.array_exists store shard_node;
    let a, g = FilesystemStore.find_child_nodes store group_node in
    FilesystemStore.erase_group_node store group_node;
    ]}

    *)

(** {1:extensions Extension Points}
    
    This library also provides custom extensions not defined in the version 3
    specification. These are tabulated below:
    {table
      {tr
        {th Extension Point}
        {th Details}}
      {tr
        {td Data Types}
        {td [char], [complex32], [int] (63-bit integer), [nativeint]}}
    }
    *)

(** {1:limitations Limitations}

    Although this implementation tries to be spec compliant, it does come with
    a few limitations:
    {ul
      {- Ocaml does not have support for unsigned integers as array data types
         and thus this library cannot support reading values of datatypes
         [uint32], [uint64] and [complex128].}
    }
    *)
