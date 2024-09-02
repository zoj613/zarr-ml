include Storage_intf
open Metadata
open Node

module Ndarray = Owl.Dense.Ndarray.Generic
module ArrayMap = Util.ArrayMap
module Indexing = Util.Indexing

module ArraySet = Set.Make (struct
  type t = int array
  let compare (x : t) (y : t) = Stdlib.compare x y
end)

module Make (Io : Types.IO) = struct
  module PartialChain = Codecs.Make(Io)
  module Deferred = Io.Deferred

  open Io
  open Deferred.Infix
  open Deferred.Syntax

  type t = Io.t

  (* All nodes are explicit upon creation so just check the node's metadata key.*)
  let group_exists t node =
    is_member t @@ GroupNode.to_metakey node

  let array_exists t node =
    is_member t @@ ArrayNode.to_metakey node

  let rec create_group ?(attrs=`Null) t node =
    group_exists t node >>= function
    | true -> Deferred.return_unit
    | false ->
      let k = GroupNode.to_metakey node in
      let* () = set t k GroupMetadata.(update_attributes default attrs |> encode) in
      GroupNode.parent node
      |> Option.fold ~none:Deferred.return_unit ~some:(create_group t)

  let create_array
    ?(sep=`Slash) ?(dimension_names=[]) ?(attributes=`Null)
    ~codecs ~shape ~chunks
    kind fv node t =
    let c = Codecs.Chain.create chunks codecs in
    let m = ArrayMetadata.create
      ~sep ~codecs:c ~dimension_names ~attributes ~shape kind fv chunks in
    let* () = set t (ArrayNode.to_metakey node) @@ ArrayMetadata.encode m in
    ArrayNode.parent node
    |> Option.fold ~none:Deferred.return_unit ~some:(create_group t)

  let group_metadata t node =
    get t @@ GroupNode.to_metakey node >>| GroupMetadata.decode

  let array_metadata t node =
    get t @@ ArrayNode.to_metakey node >>| ArrayMetadata.decode

  let node_kind t metakey =
    let+ s = get t metakey in
    match Yojson.Safe.(Util.member "node_type" @@ from_string s) with
    | `String "array" -> `Array
    | `String "group" -> `Group
    | _ -> raise @@ Parse_error (Printf.sprintf "invalid node_type in %s" metakey)

  let find_child_nodes t node =
    group_exists t node >>= function
    | false -> Deferred.return ([], [])
    | true ->
      let* _, ps = list_dir t @@ GroupNode.to_prefix node in
      Deferred.fold_left
        (fun (l, r) prefix ->
          let p = "/" ^ String.(length prefix - 1 |> sub prefix 0) in
          node_kind t (prefix ^ "zarr.json") >>| function
          | `Array  -> ArrayNode.of_path p :: l, r
          | `Group -> l, GroupNode.of_path p :: r) ([], []) ps

  let find_all_nodes t =
    let* keys = list t in
    Deferred.fold_left
      (fun ((l, r) as a) -> function
        | k when not @@ String.ends_with ~suffix:"zarr.json" k -> Deferred.return a
        | k ->
          let p = match k with
            | "zarr.json" -> "/"
            | s -> "/" ^ String.(length s - 10 |> sub s 0) in
          node_kind t k >>| function
          | `Array -> ArrayNode.of_path p :: l, r
          | `Group -> l, GroupNode.of_path p :: r) ([], []) keys

  let erase_group_node t node =
    erase_prefix t @@ GroupNode.to_prefix node

  let erase_array_node t node =
    erase_prefix t @@ ArrayNode.to_key node ^ "/"

  let erase_all_nodes t = erase_prefix t ""

  let write_array t node slice x =
    let* b = get t @@ ArrayNode.to_metakey node in
    let meta = ArrayMetadata.decode b in
    let shape = ArrayMetadata.shape meta in
    let slice_shape =
      try Indexing.slice_shape slice shape with
      | Assert_failure _ -> raise Invalid_array_slice in
    if Ndarray.shape x <> slice_shape then raise Invalid_array_slice else
    let kind = Ndarray.kind x in
    if not @@ ArrayMetadata.is_valid_kind meta kind then raise Invalid_data_type else
    let m =
      Array.fold_left
        (fun acc (co, y) ->
          let k, c = ArrayMetadata.index_coord_pair meta co in
          ArrayMap.add_to_list k (c, y) acc)
        ArrayMap.empty @@ Array.combine
          (Indexing.coords_of_slice slice shape) (Ndarray.to_array x)
    in
    let fv = ArrayMetadata.fillvalue_of_kind meta kind in
    let repr = Codecs.{kind; shape = ArrayMetadata.chunk_shape meta} in
    let prefix = ArrayNode.to_key node ^ "/" in
    let chain = ArrayMetadata.codecs meta in
    (* NOTE: there is opportunity to compute this step in parallel since
       each iteration acts on independent chunks. Maybe use Domainslib? *)
    Deferred.iter
      (fun (idx, pairs) ->
        let ckey = prefix ^ ArrayMetadata.chunk_key meta idx in
        is_member t ckey >>= function
        | true when PartialChain.is_just_sharding chain ->
          let* csize = size t ckey in
          let get_p = get_partial_values t ckey in
          let set_p = set_partial_values t ckey in
          PartialChain.partial_encode chain get_p set_p csize repr pairs
        | true ->
          let* v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr
        | false ->
          let arr = Ndarray.create repr.kind repr.shape fv in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr) (ArrayMap.bindings m)

  let read_array t node slice kind =
    let* b = get t @@ ArrayNode.to_metakey node in
    let meta = ArrayMetadata.decode b in
    if not @@ ArrayMetadata.is_valid_kind meta kind
    then raise Invalid_data_type else
    let shape = ArrayMetadata.shape meta in
    let slice_shape =
      try Indexing.slice_shape slice shape with
      | Assert_failure _ -> raise Invalid_array_slice in
    let ic = Array.mapi (fun i v -> i, v) (Indexing.coords_of_slice slice shape) in
    let m =
      Array.fold_left
        (fun acc (i, y) ->
          let k, c = ArrayMetadata.index_coord_pair meta y in
          ArrayMap.add_to_list k (i, c) acc) ArrayMap.empty ic in
    let chain = ArrayMetadata.codecs meta in
    let prefix = ArrayNode.to_key node ^ "/" in
    let fill_value = ArrayMetadata.fillvalue_of_kind meta kind in
    let repr = Codecs.{kind; shape = ArrayMetadata.chunk_shape meta} in
    (* NOTE: there is opportunity to compute this step in parallel since
       each iteration acts on independent chunks. *)
    Deferred.concat_map
      (fun (idx, pairs) ->
        let ckey = prefix ^ ArrayMetadata.chunk_key meta idx in
        is_member t ckey >>= function
        | true when PartialChain.is_just_sharding chain ->
          let get_p = get_partial_values t ckey in
          let* csize = size t ckey in
          PartialChain.partial_decode chain get_p csize repr pairs
        | true ->
          let+ v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.map (fun (i, c) -> i, Ndarray.get arr c) pairs
        | false ->
          Deferred.return @@ List.map (fun (i, _) -> i, fill_value) pairs)
      (ArrayMap.bindings m) >>| fun pairs ->
    (* sorting restores the C-order of the decoded array coordinates. *)
    let v =
      Array.of_list @@ snd @@ List.split @@
      List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
    Ndarray.of_array kind v slice_shape

  let reshape t node nshape =
    let mkey = ArrayNode.to_metakey node in
    let* b = get t mkey in
    let meta = ArrayMetadata.decode b in
    let oshape = ArrayMetadata.shape meta in
    if Array.(length nshape <> length oshape)
    then raise Invalid_resize_shape else
    let s = ArraySet.of_list @@ ArrayMetadata.chunk_indices meta oshape in
    let s' = ArraySet.of_list @@ ArrayMetadata.chunk_indices meta nshape in
    let pre = ArrayNode.to_key node ^ "/" in
    let* () =
      Deferred.iter
        (fun v ->
          let key = pre ^ ArrayMetadata.chunk_key meta v in
          is_member t key >>= function
          | true -> erase t key
          | false -> Deferred.return_unit) ArraySet.(elements @@ diff s s')
    in set t mkey @@ ArrayMetadata.(encode @@ update_shape meta nshape)
end
