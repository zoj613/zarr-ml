(* This module implements a Read-only Zip file zarr store that is Eio-aware.
   The main requirement is to implement the signature of Zarr.Types.IO.
   We use Zarr_eio's Deferred module for `Deferred` so that the store can be
   Eio-aware. Since Zip stores cannot have files updated or removed, we only
   implement the get_* and list_* family of functions and raise an
   Not_implemented exception for the set_* and erase_* family of functions.
   This effectively allows us to create a read-only store since calling any
   of the following functions would result in an `Not_implemented` exception:
     - ReadOnlyZipStore.create_group
     - ReadOnlyZipStore.create_array
     - ReadOnlyZipStore.erase_group_node
     - ReadOnlyZipStore.erase_array_node
     - ReadOnlyZipStore.erase_all_nodes
     - ReadOnlyZipStore.write_array
     - ReadOnlyZipStore.reshape
  Below we show how to implement this custom Zarr Store.

  To compile & run this example execute the command
    dune exec -- examples/zipstore.exe
  in your shell at the root of this project. *)

module ReadOnlyZipStore : sig
  exception Not_implemented

  include Zarr.Storage.STORE with type 'a Deferred.t = 'a
  val open_store : string -> t
  val close : t -> unit

end = struct
  exception Not_implemented

  module Z = struct
    module Deferred = Zarr_eio.Deferred

    type t = Zip.in_file

    let is_member t key =
      match Zip.find_entry t key with
      | exception Not_found -> false
      | _ -> true

    let size t key =
      match Zip.find_entry t key with
      | e -> e.uncompressed_size
      | exception Not_found -> 0

    let get t key =
      match Zip.find_entry t key with
      | e -> Zip.read_entry t e
      | exception Not_found -> raise (Zarr.Storage.Key_not_found key)

    let get_partial_values t key ranges =
      let data = get t key in
      let size = String.length data in
      ranges |> Eio.Fiber.List.map @@ fun (ofs, len) ->
      let f v = String.sub data ofs v in
      Option.fold ~none:(f (size - ofs)) ~some:f len

    let list t =
      Zip.entries t |> Eio.Fiber.List.filter_map @@ function
      | (e : Zip.entry) when not e.is_directory -> Some e.filename
      | _ -> None

    let list_dir t prefix =
      let module S = Set.Make(String) in
      let n = String.length prefix in
      let prefs, keys =
        List.fold_left
        (fun ((l, r) as acc) -> function
        | (e : Zip.entry) when e.is_directory -> acc
        | e when not @@ String.starts_with ~prefix e.filename -> acc
        | e when String.contains_from e.filename n '/' ->
          let key = e.filename in
          let pre = String.sub key 0 @@ 1 + String.index_from key n '/' in
          S.add pre l, r
        | e -> l, e.filename :: r) (S.empty, []) @@ Zip.entries t
      in keys, S.elements prefs

    let set _ = raise Not_implemented

    let set_partial_values _ = raise Not_implemented

    let erase _ = raise Not_implemented

    let erase_prefix _ = raise Not_implemented
  end

  (* this functor generates the public signature of our Zip file store. *)
  include Zarr.Storage.Make(Z)

  (* now we create functions to open and close the store. *)
  let open_store path = Zip.open_in path
  let close = Zip.close_in
end

let _ =
  Eio_main.run @@ fun _ ->
  let open Zarr.Metadata in
  let open Zarr.Node in

  let store = ReadOnlyZipStore.open_store "examples/data/testdata.zip" in
  let xs, _ = ReadOnlyZipStore.find_all_nodes store in
  let anode = List.hd @@ Eio.Fiber.List.filter
      (fun node -> ArrayNode.to_path node = "/some/group/name") xs in
  let meta = ReadOnlyZipStore.array_metadata store anode in
  let slice = Array.map (Fun.const @@ Owl_types.R []) (ArrayMetadata.shape meta) in
  let arr = ReadOnlyZipStore.read_array store anode slice Zarr.Ndarray.Char in
  try ReadOnlyZipStore.write_array store anode slice arr with
  | ReadOnlyZipStore.Not_implemented -> print_endline "Store is read-only";
  ReadOnlyZipStore.close store
