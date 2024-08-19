include Storage_intf
open Metadata
open Node

module Ndarray = Owl.Dense.Ndarray.Generic
module ArrayMap = Util.ArrayMap
module Indexing = Util.Indexing

module ArraySet = Set.Make (struct
  type t = int array
  let compare = Stdlib.compare
end)

module Make (Io : Types.IO) = struct
  module PartialChain = Codecs.Make(Io)
  module Deferred = Io.Deferred

  open Io
  open Deferred.Infix

  type t = Io.t

  (* All nodes are explicit upon creation so just check the node's metadata key.*)
  let group_exists t node =
    is_member t @@ GroupNode.to_metakey node

  let array_exists t node =
    is_member t @@ ArrayNode.to_metakey node

  let rec create_group ?(attrs=`Null) t node =
    group_exists t node >>= function
    | true -> Deferred.return ()
    | false ->
      let k = GroupNode.to_metakey node in
      set t k GroupMetadata.(update_attributes default attrs |> encode) >>= fun () ->
      match GroupNode.parent node with
      | None -> Deferred.return ()
      | Some n -> create_group t n

  let create_array
    ?(sep=`Slash) ?(dimension_names=[]) ?(attributes=`Null)
    ~codecs ~shape ~chunks
    kind fv node t =
    let c = Codecs.Chain.create chunks codecs in
    let m = ArrayMetadata.create
      ~sep ~codecs:c ~dimension_names ~attributes ~shape kind fv chunks in
    set t (ArrayNode.to_metakey node) @@ ArrayMetadata.encode m >>= fun () ->
    create_group t @@ ArrayNode.parent node

  let group_metadata t node =
    get t @@ GroupNode.to_metakey node >>| GroupMetadata.decode

  let array_metadata t node =
    get t @@ ArrayNode.to_metakey node >>| ArrayMetadata.decode

  let node_type t metakey =
    let open Yojson.Safe in
    get t metakey >>| fun s ->
    Util.to_string @@ Util.member "node_type" @@ from_string s

  let find_child_nodes t node =
    list_dir t @@ GroupNode.to_prefix node >>= fun (_, gp) ->
    Deferred.fold_left
      (fun (l, r) pre ->
        node_type t @@ pre ^ "zarr.json" >>| fun nt ->
        let p = "/" ^ String.(length pre - 1 |> sub pre 0) in
        if nt = "array" then ArrayNode.of_path p :: l, r
        else l, GroupNode.of_path p :: r) ([], []) gp

  let find_all_nodes t =
    list t >>= fun keys ->
    Deferred.fold_left
      (fun ((l, r) as acc) k ->
        if String.ends_with ~suffix:"/zarr.json" k then
          node_type t k >>| fun nt ->
          let p = "/" ^ String.(length k - 10 |> sub k 0) in
          if nt = "array" then ArrayNode.of_path p :: l, r
          else l, GroupNode.of_path p :: r
        else Deferred.return acc) ([], []) keys >>| fun acc ->
    match acc with
    | [], [] as xs -> xs
    | l, r -> l, GroupNode.root :: r

  let erase_group_node t node =
    erase_prefix t @@ GroupNode.to_prefix node

  let erase_array_node t node =
    erase_prefix t @@ ArrayNode.to_key node ^ "/"

  let erase_all_nodes t =
    list t >>= Deferred.iter (erase t)

  let write_array t node slice x =
    get t @@ ArrayNode.to_metakey node >>| ArrayMetadata.decode >>= fun meta ->
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
          size t ckey >>= fun csize ->
          let get_p = get_partial_values t ckey in
          let set_p = set_partial_values t ckey in
          PartialChain.partial_encode chain get_p set_p csize repr pairs
        | true ->
          get t ckey >>= fun v ->
          let arr = Codecs.Chain.decode chain repr v in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr
        | false ->
          let arr = Ndarray.create repr.kind repr.shape fv in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr) (ArrayMap.bindings m)

  let read_array t node slice kind =
    get t @@ ArrayNode.to_metakey node >>| ArrayMetadata.decode >>= fun meta ->
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
          size t ckey >>= fun csize ->
          PartialChain.partial_decode chain get_p csize repr pairs
        | true ->
          get t ckey >>| Codecs.Chain.decode chain repr >>| fun arr ->
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
    get t mkey >>| ArrayMetadata.decode >>= fun meta ->
    let oshape = ArrayMetadata.shape meta in
    if Array.(length nshape <> length oshape)
    then raise Invalid_resize_shape else
    let s = ArraySet.of_list @@ ArrayMetadata.chunk_indices meta oshape in
    let s' = ArraySet.of_list @@ ArrayMetadata.chunk_indices meta nshape in
    let pre = ArrayNode.to_key node ^ "/" in
    Deferred.iter
      (fun v ->
        let key = pre ^ ArrayMetadata.chunk_key meta v in
        is_member t key >>= function
        | true -> erase t key
        | false -> Deferred.return ()) ArraySet.(diff s s' |> elements)
    >>= fun () ->
    set t mkey @@ ArrayMetadata.(encode @@ update_shape meta nshape)
end
