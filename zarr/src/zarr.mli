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

module Metadata = Metadata

(** {1 Storage} *)

module Storage = Storage
module Memory = Memory
module Types = Types

(** {1 Codecs} *)

module Codecs = Codecs

(** {1 Indexing} *)

module Indexing = Util.Indexing

(** {1 Utils} *)

module Util = Util

(** {1:examples Examples}
    
    {2:create_array Create, read & write array.}
    Here we show how the library's asynchronous API using Lwt's concurrency monad can be used.
    {@ocaml[
    open Zarr.Metadata
    open Zarr.Node
    open Zarr.Codecs
    open Zarr_lwt.Storage
    open FilesystemStore.Deferred.Syntax

    let _ =
      Lwt_main.run begin
        let store = FilesystemStore.create "testdata.zarr" in
        let group_node = GroupNode.of_path "/some/group" in
        let* () = FilesystemStore.create_group store group_node in
        let array_node = ArrayNode.(group_node / "name") in
        let* () = FilesystemStore.create_array
          ~codecs:[`Bytes BE] ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
          Bigarray.Float32 Float.neg_infinity array_node store in
        let slice = Owl_types.[|R [0; 20]; I 10; R []|] in
        let* x = FilesystemStore.read_array store array_node slice Bigarray.Float32 in
        let x' = Owl.Dense.Ndarray.Generic.map (fun _ -> Owl_stats_dist.uniform_rvs 0. 10.) x
        in FilesystemStore.write_array store array_node slice x'
      end
    ]} *)

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
