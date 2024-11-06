include Storage_intf

module ArrayMap = Util.ArrayMap
module Indexing = Ndarray.Indexing

module ArraySet = Set.Make (struct
  type t = int array
  let compare (x : t) (y : t) = Stdlib.compare x y
end)

module Make (Io : Types.IO) = struct
  module Io_chain = Codecs.Make(Io)
  module Deferred = Io.Deferred

  open Io
  open Deferred.Infix
  open Deferred.Syntax

  type t = Io.t

  let node_kind t metakey =
    let+ s = get t metakey in
    match Yojson.Safe.(Util.member "node_type" @@ from_string s) with
    | `String "array" -> `Array
    | `String "group" -> `Group
    | _ -> raise (Metadata.Parse_error (Printf.sprintf "invalid node_type in %s" metakey))

  let choose ~path ~left ~right = function
    | `Array -> Node.Array.of_path path :: left, right
    | `Group -> left, Node.Group.of_path path :: right

  let hierarchy t =
    let add ((left, right) as acc) k =
      if not (String.ends_with ~suffix:"zarr.json" k) then Deferred.return acc else
      let path = if k = "zarr.json" then "/" else "/" ^ String.(sub k 0 (length k - 10)) in
      let+ kind = node_kind t k in
      choose ~path ~left ~right kind
    in
    list t >>= Deferred.fold_left add ([], [])

  let clear t = erase_prefix t ""

  module Group = struct
    let exists t node = is_member t (Node.Group.to_metakey node)

    (* This recursively creates parent group nodes if they don't exist.*)
    let rec create ?(attrs=`Null) t node =
      exists t node >>= function
      | true -> Deferred.return_unit
      | false ->
        let k = Node.Group.to_metakey node in
        let* () = set t k Metadata.Group.(update_attributes default attrs |> encode) in
        match Node.Group.parent node with
        | None -> Deferred.return_unit
        | Some p -> create t p

    let metadata t node =
      let+ data = get t (Node.Group.to_metakey node) in
      Metadata.Group.decode data

    let children t node =
      let add (left, right) prefix =
        let path = "/" ^ String.sub prefix 0 (String.length prefix - 1) in
        let+ kind = node_kind t (prefix ^ "zarr.json") in
        choose ~path ~left ~right kind
      in
      exists t node >>= function
      | false -> Deferred.return ([], [])
      | true ->
        let* _, ps = list_dir t (Node.Group.to_prefix node) in
        Deferred.fold_left add ([], []) ps

    let delete t node = erase_prefix t (Node.Group.to_prefix node)

    let rename t node str =
      let key = Node.Group.to_key node in
      let key' = Node.Group.(rename node str |> to_key) in
      exists t node >>= function
      | false -> raise (Key_not_found key)
      | true -> rename t key key'
  end
  
  module Array = struct
    let exists t node = is_member t (Node.Array.to_metakey node)

    (* This recursively creates parent group nodes if they don't exist.*)
    let create
      ?(sep=`Slash) ?(dimension_names=[]) ?(attributes=`Null)
      ~codecs ~shape ~chunks
      kind fv node t =
      let c = Codecs.Chain.create chunks codecs in
      let m = Metadata.Array.create
        ~sep ~codecs:c ~dimension_names ~attributes ~shape kind fv chunks in
      let key = Node.Array.to_metakey node in
      let value = Metadata.Array.encode m in 
      let* () = set t key value in
      Option.fold ~none:Deferred.return_unit ~some:(Group.create t) (Node.Array.parent node)

    let metadata t node =
      let+ data = get t (Node.Array.to_metakey node) in
      Metadata.Array.decode data

    let delete t node = erase_prefix t (Node.Array.to_key node ^ "/")

    let write t node slice x =
      let add_coord_value ~meta acc (co, y) =
        let chunk_idx, c = Metadata.Array.index_coord_pair meta co in
        ArrayMap.add_to_list chunk_idx (c, y) acc
      in
      let update_chunk ~t ~meta ~prefix ~chain ~fv ~repr (idx, pairs) =
        let ckey = prefix ^ Metadata.Array.chunk_key meta idx in
        if Io_chain.is_just_sharding chain then
          let* shardsize = size t ckey in
          let pget = get_partial_values t ckey in
          let pset = set_partial_values t ckey in
          Io_chain.partial_encode chain pget pset shardsize repr pairs fv
        else is_member t ckey >>= function
        | true ->
          let* v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey (Codecs.Chain.encode chain arr)
        | false ->
          let arr = Ndarray.create repr.kind repr.shape fv in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey (Codecs.Chain.encode chain arr)
      in
      let* meta = metadata t node in
      let shape = Metadata.Array.shape meta in
      let slice_shape = try Indexing.slice_shape slice shape with
        | Assert_failure _ -> raise Invalid_array_slice
      in
      if Ndarray.shape x <> slice_shape then raise Invalid_array_slice else
      let kind = Ndarray.data_type x in
      if not (Metadata.Array.is_valid_kind meta kind) then raise Invalid_data_type else
      let coords = Indexing.coords_of_slice slice shape in
      let coord_value_pair = Array.combine coords (Ndarray.to_array x) in
      let m = Array.fold_left (add_coord_value ~meta) ArrayMap.empty coord_value_pair in
      let fv = Metadata.Array.fillvalue_of_kind meta kind in
      let repr = Codecs.{kind; shape = Metadata.Array.chunk_shape meta} in
      let prefix = Node.Array.to_key node ^ "/" in
      let chain = Metadata.Array.codecs meta in
      let bindings = ArrayMap.bindings m in
      Deferred.iter (update_chunk ~t ~meta ~prefix ~chain ~fv ~repr) bindings

    let read :
      type a. t ->
      Node.Array.t ->
      Indexing.index array ->
      a Ndarray.dtype ->
      a Ndarray.t Deferred.t
      = fun t node slice kind ->
      let add_indexed_coord ~meta acc (i, y) =
        let chunk_idx, c = Metadata.Array.index_coord_pair meta y in
        ArrayMap.add_to_list chunk_idx (i, c) acc
      in
      let read_chunk ~t ~meta ~prefix ~chain ~fv ~repr (idx, pairs) =
        let ckey = prefix ^ Metadata.Array.chunk_key meta idx in
        size t ckey >>= function
        | 0 -> Deferred.return @@ List.map (fun (i, _) -> i, fv) pairs
        | shardsize when Io_chain.is_just_sharding chain ->
          let pget = get_partial_values t ckey in
          Io_chain.partial_decode chain pget shardsize repr pairs fv
        | _ ->
          let+ v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.map (fun (i, c) -> i, Ndarray.get arr c) pairs
      in
      let* meta = metadata t node in
      if not (Metadata.Array.is_valid_kind meta kind) then raise Invalid_data_type else
      let shape = Metadata.Array.shape meta in
      let slice_shape = try Indexing.slice_shape slice shape with
        | Assert_failure _ -> raise Invalid_array_slice
      in
      let icoords = Array.mapi (fun i v -> i, v) (Indexing.coords_of_slice slice shape) in
      let m = Array.fold_left (add_indexed_coord ~meta) ArrayMap.empty icoords in
      let chain = Metadata.Array.codecs meta in
      let prefix = Node.Array.to_key node ^ "/" in
      let fv = Metadata.Array.fillvalue_of_kind meta kind in
      let repr = Codecs.{kind; shape = Metadata.Array.chunk_shape meta} in
      let bs = ArrayMap.bindings m in
      let+ ps = Deferred.concat_map (read_chunk ~t ~meta ~prefix ~chain ~fv ~repr) bs in
      (* sorting restores the C-order of the decoded array coordinates.*)
      let sorted_pairs = List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) ps in
      let vs = List.map snd sorted_pairs in
      Ndarray.of_array kind slice_shape (Array.of_list vs)

    let reshape t node new_shape =
      let remove ~t ~meta ~prefix v =
        let key = prefix ^ Metadata.Array.chunk_key meta v in
        is_member t key >>= function
        | false -> Deferred.return_unit
        | true -> erase t key
      in
      let mkey = Node.Array.to_metakey node in
      let* meta = metadata t node in
      let old_shape = Metadata.Array.shape meta in
      if Array.(length new_shape <> length old_shape) then raise Invalid_resize_shape else
      let s = ArraySet.of_list (Metadata.Array.chunk_indices meta old_shape) in
      let s' = ArraySet.of_list (Metadata.Array.chunk_indices meta new_shape) in
      let prefix = Node.Array.to_key node ^ "/" in
      let unreachable_chunks = ArraySet.elements (ArraySet.diff s s') in 
      let* () = Deferred.iter (remove ~t ~meta ~prefix) unreachable_chunks in
      set t mkey Metadata.Array.(encode @@ update_shape meta new_shape)

    let rename t node str =
      let key = Node.Array.to_key node in
      let key' = Node.Array.(rename node str |> to_key) in
      exists t node >>= function
      | false -> raise (Key_not_found key)
      | true -> rename t key key'
  end
end
