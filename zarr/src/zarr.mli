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
module Zip = Zip_archive
module Types = Types

(** {1 Codecs} *)

module Codecs = Codecs

(** {1 Indexing} *)

module Indexing = Ndarray.Indexing

(** {1 Utils} *)

module Util = Util

(** {1 Ndarray} *)

module Ndarray = Ndarray

(** {1:examples Examples}
    
    {2:create_array Create, read & write array.}
    Here we show how the library's asynchronous API using Lwt's concurrency monad can be used.
    {@ocaml[
    open Zarr
    open Zarr.Ndarray
    open Zarr.Indexing
    open Zarr.Codecs
    open Zarr_lwt.Storage
    open FilesystemStore.Deferred.Syntax

    let _ =
      Lwt_main.run begin
        let store = FilesystemStore.create "testdata.zarr" in
        let group_node = Node.Group.root in
        let* () = FilesystemStore.Group.create group_node in
        let array_node = ArrayNode.(group_node / "name") in
        let* () = FilesystemStore.Array.create
          ~codecs:[`Bytes BE] ~shape:[|100; 100; 50|] ~chunks:[|10; 15; 20|]
          Ndarray.Float32 Float.neg_infinity array_node store in
        let slice = [|R [|0; 20|]; I 10; L [||]|] in
        let* x = FilesystemStore.Array.read store array_node slice Ndarray.Float32 in
        let x' = Ndarray.map (fun _ -> Random.int 11 |> Float.of_int) x
        in FilesystemStore.Array.write store array_node slice x'
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
      {- Currently we do not support the following data types: [float16],
      [uint32], [complex128], [r*], and variable length strings.}
    }
    *)
