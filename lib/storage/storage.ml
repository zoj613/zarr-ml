include Storage_intf

open Util.Result_syntax
open Node

module Ndarray = Owl.Dense.Ndarray.Generic
module ArraySet = Util.ArraySet
module Arraytbl = Util.Arraytbl
module AM = Metadata.ArrayMetadata
module GM = Metadata.GroupMetadata

module Make (M : STORE) : S with type t = M.t = struct
  include M

  (* All nodes are explicit upon creation so just check the node's metadata key.*)
  let group_exists t node =
    M.is_member t @@ GroupNode.to_metakey node

  let array_exists t node =
    M.is_member t @@ ArrayNode.to_metakey node

  let rec create_group ?metadata t node =
    if group_exists t node then ()
    else
      (match metadata, GroupNode.to_metakey node with
      | Some m, k -> set t k @@ GM.encode m;
      | None, k -> set t k @@ GM.(default |> encode));
      make_implicit_groups_explicit t @@ GroupNode.parent node

  and make_implicit_groups_explicit t = function
    | None -> ()
    | Some n -> create_group t n

  let create_array
    ?(sep=`Slash)
    ?(dimension_names=[])
    ?(attributes=`Null)
    ?codecs
    ~shape
    ~chunks
    kind
    fill_value
    node
    t
    =
    let open Util in
    let repr = {kind; fill_value; shape = chunks} in
    (match codecs with
    | Some c -> Codecs.Chain.create repr c
    | None -> Ok Codecs.Chain.default)
    >>= fun codecs ->
    AM.create
      ~sep ~codecs ~dimension_names ~attributes ~shape
      kind fill_value chunks
    >>| AM.encode >>| set t (ArrayNode.to_metakey node) >>| fun () ->
    make_implicit_groups_explicit t @@ Some (ArrayNode.parent node)

  let group_metadata node t =
    get t @@ GroupNode.to_metakey node >>= fun bytes ->
    GM.decode bytes >>? fun msg -> `Store_read msg

  let array_metadata node t =
    get t @@ ArrayNode.to_metakey node >>= fun bytes ->
    AM.decode bytes >>? fun msg -> `Store_read msg

  (* Assumes without checking that [metakey] is a valid node metadata key.*)
  let unsafe_node_type t metakey =
    let open Yojson.Safe in
    get t metakey |> Result.get_ok |> from_string
    |> Util.member "node_type" |> Util.to_string

  let find_child_nodes t node =
    List.fold_left
      (fun (l, r) pre ->
        let p = "/" ^ String.(length pre - 1 |> sub pre 0) in
        if unsafe_node_type t (pre ^ "zarr.json") = "array" then
          (Result.get_ok @@ ArrayNode.of_path p) :: l, r
        else
          l, (Result.get_ok @@ GroupNode.of_path p) :: r)
      ([], []) (snd @@ list_dir t @@ GroupNode.to_prefix node)

  let find_all_nodes t =
    match
      List.fold_left
        (fun ((l, r) as acc) key ->
          if String.ends_with ~suffix:"/zarr.json" key then
            let p = "/" ^ String.(length key - 10 |> sub key 0) in
            if unsafe_node_type t key = "array" then
              (Result.get_ok @@ ArrayNode.of_path p) :: l, r
            else
              l, (Result.get_ok @@ GroupNode.of_path p) :: r
          else acc) ([], []) (list_prefix "" t)
    with
    | [], [] as xs -> xs
    | l, r -> l, GroupNode.root :: r

  let erase_group_node t node =
    erase_prefix t @@ GroupNode.to_prefix node

  let erase_array_node t node =
    erase t @@ ArrayNode.to_metakey node

  let erase_all_nodes t = erase_prefix t ""

  let set_array
  : type a b.
    ArrayNode.t ->
    Owl_types.index array ->
    (a, b) Ndarray.t ->
    t ->
    (unit, [> error ]) result
  = fun node slice x t ->
    let open Util in
    get t @@ ArrayNode.to_metakey node >>= fun bytes ->
    AM.decode bytes >>? (fun msg -> `Store_write msg) >>= fun meta ->
    let arr_shape = AM.shape meta in
    (if Ndarray.shape x = Indexing.slice_shape slice arr_shape then 
        Ok ()
      else
        Error (`Store_write "slice and input array shapes are unequal."))
    >>= fun () ->
    let kind = Ndarray.kind x in
    (if AM.is_valid_kind meta kind then
        Ok ()
      else
       Result.error @@
       `Store_write (
         "input array's kind is not compatible with node's data type."))
    >>= fun () ->
    let coords = Indexing.coords_of_slice slice arr_shape in
    let tbl = Arraytbl.create @@ Array.length coords in
    Ndarray.iteri (fun i y ->
      let k, c = AM.index_coord_pair meta coords.(i) in
      Arraytbl.add tbl k (c, y)) x;
    let repr =
      {kind
      ;shape = AM.chunk_shape meta
      ;fill_value = AM.fillvalue_of_kind meta kind}
    in
    let chain = AM.codecs meta in
    let prefix = ArrayNode.to_key node ^ "/" in
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun () ->
        let ckey = prefix ^ AM.chunk_key meta idx in
        let pairs = Arraytbl.find_all tbl idx in 
        match get t ckey with
        | Ok b when Codecs.Chain.is_just_sharding chain ->
          Codecs.Chain.partial_encode chain repr pairs b >>| set t ckey
        | Ok b ->
          Codecs.Chain.decode chain repr b >>= fun arr ->
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          Codecs.Chain.encode chain arr >>| set t ckey
        | Error _ ->
          (* array chunk not present in store, so create one. *)
          let arr = Ndarray.create repr.kind repr.shape repr.fill_value in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          Codecs.Chain.encode chain arr >>| set t ckey)
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok ())

  let get_array
  : type a b.
    ArrayNode.t ->
    Owl_types.index array ->
    (a, b) Bigarray.kind ->
    t ->
    ((a, b) Ndarray.t, [> error]) result
  = fun node slice kind t ->
    let open Util in
    get t @@ ArrayNode.to_metakey node >>= fun bytes ->
    AM.decode bytes >>? (fun msg -> `Store_read msg) >>= fun meta ->
    (if AM.is_valid_kind meta kind then
        Ok ()
      else
       Result.error @@
       `Store_read ("input kind is not compatible with node's data type."))
    >>= fun () ->
    let arr_shape = AM.shape meta in
    (try Result.ok @@ Indexing.slice_shape slice arr_shape with
    | Assert_failure _ -> 
      Result.error @@
      `Store_read "slice shape is not compatible with node's shape.")
    >>= fun slice_shape ->
    let coords = Indexing.coords_of_slice slice arr_shape in
    let tbl = Arraytbl.create @@ Array.length coords in
    Array.iteri
      (fun i y ->
        let k, c = AM.index_coord_pair meta y in
        Arraytbl.add tbl k (i, c)) coords;
    let chain = AM.codecs meta in
    let prefix = ArrayNode.to_key node ^ "/" in
    let fill_value = AM.fillvalue_of_kind meta kind in
    let repr = {kind; fill_value; shape = AM.chunk_shape meta} in
    ArraySet.fold
      (fun idx acc ->
        acc >>= fun xs ->
        let pairs = Arraytbl.find_all tbl idx in
        match get t @@ prefix ^ AM.chunk_key meta idx with
        | Ok b when Codecs.Chain.is_just_sharding chain ->
          Codecs.Chain.partial_decode chain repr pairs b >>| List.append xs
        | Ok b ->
          Codecs.Chain.decode chain repr b >>| fun arr ->
          List.fold_left
            (fun accu (i, c) -> (i, Ndarray.get arr c) :: accu) xs pairs
       | Error _ ->
          Result.ok @@
          List.fold_left
            (fun accu (i, _) -> (i, repr.fill_value) :: accu) xs pairs)
      (ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl) (Ok []) 
    >>| fun pairs ->
    let vals =
      Array.of_list @@ snd @@ List.split @@
      (* restores the C-order of the decoded array coordinates. *)
      List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs
    in Ndarray.of_array kind vals slice_shape

  let reshape t node shape =
    let mkey = ArrayNode.to_metakey node in
    get t mkey  >>= fun bytes ->
    AM.decode bytes >>? (fun msg -> `Store_write msg)
    >>= fun meta ->
    (if Array.length shape = Array.length @@ AM.shape meta then
      Ok ()
    else
      Error (`Store_write "new shape must have same number of dimensions."))
    >>| fun () ->
    let pre = ArrayNode.to_key node ^ "/" in
    let s =
      ArraySet.of_list @@ AM.chunk_indices meta @@ AM.shape meta in
    let s' =
      ArraySet.of_list @@ AM.chunk_indices meta shape in
    ArraySet.iter
      (fun v -> erase t @@ pre ^ AM.chunk_key meta v)
      ArraySet.(diff s s');
    set t mkey @@ AM.encode @@ AM.update_shape meta shape
end

module MemoryStore = struct
  module MS = Make (Memory.Impl)
  let create = Memory.create
  include MS
end

module FilesystemStore = struct
  module FS = Make (Filesystem.Impl)
  let create = Filesystem.create
  let open_store = Filesystem.open_store
  let open_or_create = Filesystem.open_or_create
  include FS
end
