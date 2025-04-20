include Storage_intf

module Make (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) = struct
  module IO_chain = Codecs.Make(IO)(Store)
  type error = Store.error
  type t = Store.t

  open IO.Infix
  open IO.Syntax

  let node_kind t metakey =
    let* x = Store.get t metakey in
    match Yojson.Safe.(Util.member "node_type" @@ from_string x) with
    | `String "array" -> IO.return `Array
    | `String "group" -> IO.return `Group
    | _ -> IO.error (`Parse_error (Printf.sprintf "invalid node_type in %s" metakey))

  let choose path left right = function
    | `Array -> IO.lift (Result.map (fun x -> x :: left, right) (Node.Array.of_path path))
    | `Group -> IO.lift (Result.map (fun x -> left, x :: right) (Node.Group.of_path path))

  let hierarchy t =
    let maybe_add ~t acc = function
      | "zarr.json" as key -> IO.lift acc >>= fun (l, r) -> node_kind t key >>= choose "/" l r
      | key -> match Filename.chop_suffix_opt ~suffix:"/zarr.json" ("/" ^ key) with
        | None -> IO.lift acc
        | Some path -> IO.lift acc >>= fun (l, r) -> node_kind t key >>= choose path l r
    in
    Store.list t >>= IO.fold_left (maybe_add ~t) (Ok ([], []))

  let clear t = Store.erase_prefix t ""

  module Group = struct
    let exists t node = Store.is_member t (Node.Group.to_metakey node)
    let delete t node = Store.erase_prefix t (Node.Group.to_prefix node)

    (* This recursively creates parent group nodes if they don't exist.*)
    let rec create ?(attrs=`Null) t node =
      exists t node >>= function
      | true -> IO.return_unit
      | false ->
        let meta = Metadata.Group.(update_attributes default attrs) in
        let* () = Store.set t (Node.Group.to_metakey node) (Metadata.Group.encode meta) in
        Option.fold ~none:IO.return_unit ~some:(create t) (Node.Group.parent node)

    let metadata t node =
      let* x = Store.get t (Node.Group.to_metakey node) in
      IO.lift (Metadata.Group.decode x)

    let rename t node str =
      let key = Node.Group.to_key node in
      exists t node >>= function
      | false -> IO.error (`Key_not_found key)
      | true ->
        let* node' = IO.lift (Node.Group.rename node str) in
        let* () = Store.rename t key (Node.Group.to_key node') in
        IO.return node'

    let children t node =
      let add_node ~t acc prefix =
        let path = "/" ^ Filename.chop_suffix prefix "/" in
        let* k = node_kind t (prefix ^ "zarr.json") in
        IO.bind (IO.lift acc) (fun (l, r) -> choose path l r k)
      in
      let xs = ([], []) in
      exists t node >>= function
      | false -> IO.return xs
      | true ->
        let* _, ps = Store.list_dir t (Node.Group.to_prefix node) in
        IO.fold_left (add_node ~t) (Ok xs) ps
  end
  
  module Array = struct
    module CoordMap = Util.CoordMap
    module Indexing = Ndarray.Indexing
    let exists t node = Store.is_member t (Node.Array.to_metakey node)
    let delete t node = Store.erase_prefix t (Node.Array.to_key node ^ "/")

    (* This recursively creates parent group nodes if they don't exist.*)
    let create ?(overwrite=false) ?(sep=`Slash) ?(attributes=`Null) ?dimension_names ~codecs ~shape ~chunks kind fv node t =
      let write_metadata_json () =
        let create c = Metadata.Array.create ?dimension_names ~sep ~codecs:c ~attributes ~shape kind fv chunks in
        let maybe_metadata = Result.bind (Codecs.Chain.create chunks codecs) create in
        let* x = IO.lift (Result.map Metadata.Array.encode maybe_metadata) in
        let* () = Store.set t (Node.Array.to_metakey node) x in
        Option.fold ~none:IO.return_unit ~some:(Group.create t) (Node.Array.parent node)
      in
      exists t node >>= function
      | false -> write_metadata_json ()
      | true -> match overwrite with
        | true -> delete t node >>= write_metadata_json
        | false -> IO.error (`Node_already_exists (Node.Array.to_path node))

    let metadata t node =
      let* x = Store.get t (Node.Array.to_metakey node) in
      IO.lift (Metadata.Array.decode x)

    let write t node indices x =
      let update_ndarray ~arr (c, v) = Ndarray.set arr c v in
      let add_coord_value ~meta acc co y =
        let chunk_idx, c = Metadata.Array.index_coord_pair meta co in
        CoordMap.add_to_list chunk_idx (c, y) acc
      in
      let update_chunk ~t ~meta ~prefix ~chain ~fill_value ~repr k (idx, pairs) =
        let* () = IO.lift k in
        let chunk_key = prefix ^ Metadata.Array.chunk_key meta idx in
        if IO_chain.is_just_sharding chain
        then IO_chain.partial_encode ~fill_value t chunk_key chain repr pairs
        else Store.is_member t chunk_key >>= function
        | false ->
          let arr = Ndarray.create repr.datatype repr.shape fill_value in
          List.iter (update_ndarray ~arr) pairs;
          Store.set t chunk_key (Codecs.Chain.encode chain arr)
        | true ->
          let* v = Store.get t chunk_key in
          let arr = Codecs.Chain.decode chain repr v in
          List.iter (update_ndarray ~arr) pairs;
          Store.set t chunk_key (Codecs.Chain.encode chain arr)
      in
      let* meta = metadata t node in
      let datatype = Ndarray.data_type x in
      let* fill_value = IO.lift (Metadata.Array.fill_value meta datatype) in
      let shape = Metadata.Array.shape meta in
      let* slice = IO.lift (Indexing.create indices shape) in
      let slice_shape = Indexing.slice_shape slice in
      if Ndarray.shape x <> slice_shape then IO.error `Invalid_array_slice else
      let coords = Indexing.coords_of_slice slice in
      let m = List.fold_left2 (add_coord_value ~meta) CoordMap.empty coords (Ndarray.to_array x |> Array.to_list)
      and repr = Codecs.{datatype; shape = Metadata.Array.chunk_shape meta}
      and prefix = Node.Array.to_key node ^ "/"
      and chain = Metadata.Array.codecs meta in
      IO.fold_left (update_chunk ~t ~meta ~prefix ~chain ~fill_value ~repr) (Ok ()) (CoordMap.bindings m)

    let read (type a) t node indices (datatype : a Ndarray.dtype) =
      let add_indexed_coord ~meta acc i y =
        let chunk_idx, c = Metadata.Array.index_coord_pair meta y in
        CoordMap.add_to_list chunk_idx (i, c) acc
      in
      let read_chunk ~t ~meta ~prefix ~chain ~fill_value ~repr acc (idx, pairs) =
        let* xs = IO.lift acc in
        let ckey = prefix ^ Metadata.Array.chunk_key meta idx in
        Store.size t ckey >>= function
        | 0 -> IO.return (xs @ List.map (fun (i, _) -> i, fill_value) pairs)
        | _ when IO_chain.is_just_sharding chain ->
          IO.map (List.append xs) (IO_chain.partial_decode ~fill_value t ckey chain repr pairs)
        | _ ->
          let+ v = Store.get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          xs @ List.map (fun (i, c) -> i, Ndarray.get arr c) pairs
      in
      let* meta = metadata t node in
      let* fill_value = IO.lift (Metadata.Array.fill_value meta datatype) in
      let shape = Metadata.Array.shape meta in
      let* slice = IO.lift (Indexing.create indices shape) in
      let slice_shape = Indexing.slice_shape slice in
      let numel = List.fold_left Int.mul 1 slice_shape
      and coords = Indexing.coords_of_slice slice in
      let m = List.fold_left2 (add_indexed_coord ~meta) CoordMap.empty List.(init numel Fun.id) coords
      and chain = Metadata.Array.codecs meta
      and prefix = Node.Array.to_key node ^ "/"
      and repr = Codecs.{datatype; shape = Metadata.Array.chunk_shape meta} in
      let+ ps = IO.fold_left (read_chunk ~t ~meta ~prefix ~chain ~fill_value ~repr) (Ok []) (CoordMap.bindings m) in
      (* sorting restores the C-order of the decoded array coordinates.*)
      let ps' = List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) ps in
      let vs = List.map snd ps' in
      Ndarray.of_array datatype slice_shape (Array.of_list vs)

    module StrSet = Set.Make (struct
      type t = int list
      let compare : t -> t -> int = Stdlib.compare
    end)

    let resize t node new_shape =
      let remove ~t ~meta ~prefix acc v =
        let* () = IO.lift acc in
        let key = prefix ^ Metadata.Array.chunk_key meta v in
        Store.is_member t key >>= function
        | false -> IO.return_unit
        | true -> Store.erase t key
      in
      let* meta = metadata t node in
      let old_shape = Metadata.Array.shape meta in
      if List.(length new_shape <> length old_shape) then IO.error `Invalid_resize_shape else
      let s = StrSet.of_list (Metadata.Array.chunk_indices meta old_shape)
      and s' = StrSet.of_list (Metadata.Array.chunk_indices meta new_shape) in
      let xs = StrSet.(diff s s' |> elements) in  (* unreachable chunks after reshaping *)
      let prefix = Node.Array.to_key node ^ "/" in
      let* () = IO.fold_left (remove ~t ~meta ~prefix) (Ok ()) xs in
      Store.set t (Node.Array.to_metakey node) Metadata.Array.(encode @@ update_shape meta new_shape)

    let rename t node str =
      let key = Node.Array.to_key node in
      exists t node >>= function
      | false -> IO.error (`Key_not_found key)
      | true ->
        let* node' = IO.lift (Node.Array.rename node str) in
        let* () = Store.rename t key (Node.Array.to_key node') in
        IO.return node'
  end
end
