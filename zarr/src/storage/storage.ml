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
    | _ -> raise @@ Metadata.Parse_error (Printf.sprintf "invalid node_type in %s" metakey)

  let hierarchy t =
    list t >>= Deferred.fold_left
      (fun ((l, r) as a) -> function
        | k when not @@ String.ends_with ~suffix:"zarr.json" k -> Deferred.return a
        | k ->
          let p = match k with
            | "zarr.json" -> "/"
            | s -> "/" ^ String.(length s - 10 |> sub s 0) in
          node_kind t k >>| function
          | `Array -> Node.Array.of_path p :: l, r
          | `Group -> l, Node.Group.of_path p :: r) ([], [])

  let clear t = erase_prefix t ""

  module Group = struct
    let exists t node =
      is_member t @@ Node.Group.to_metakey node

    let rec create ?(attrs=`Null) t node =
      exists t node >>= function
      | true -> Deferred.return_unit
      | false ->
        let k = Node.Group.to_metakey node in
        let* () = set t k Metadata.Group.(update_attributes default attrs |> encode) in
        Node.Group.parent node |> Option.fold ~none:Deferred.return_unit ~some:(create t)

    let metadata t node =
      get t @@ Node.Group.to_metakey node >>| Metadata.Group.decode

    let children t node =
      exists t node >>= function
      | false -> Deferred.return ([], [])
      | true ->
        let* _, ps = list_dir t @@ Node.Group.to_prefix node in
        Deferred.fold_left
          (fun (l, r) prefix ->
            let p = "/" ^ String.(length prefix - 1 |> sub prefix 0) in
            node_kind t (prefix ^ "zarr.json") >>| function
            | `Array  -> Node.Array.of_path p :: l, r
            | `Group -> l, Node.Group.of_path p :: r) ([], []) ps

    let delete t node =
      erase_prefix t @@ Node.Group.to_prefix node

    let rename t node str =
      let key = Node.Group.to_key node in
      exists t node >>= function
      | false -> raise @@ Key_not_found key
      | true -> rename t key Node.Group.(rename node str |> to_key)
  end
  
  module Array = struct
    let exists t node =
      is_member t @@ Node.Array.to_metakey node

    let create
      ?(sep=`Slash) ?(dimension_names=[]) ?(attributes=`Null) ~codecs ~shape ~chunks
      kind fv node t =
      let c = Codecs.Chain.create chunks codecs in
      let m = Metadata.Array.create
        ~sep ~codecs:c ~dimension_names ~attributes ~shape kind fv chunks in
      let* () = set t (Node.Array.to_metakey node) @@ Metadata.Array.encode m in
      Node.Array.parent node |> Option.fold ~none:Deferred.return_unit ~some:(Group.create t)

    let metadata t node =
      get t @@ Node.Array.to_metakey node >>| Metadata.Array.decode

    let delete t node =
      erase_prefix t @@ Node.Array.to_key node ^ "/"

    let write t node slice x =
      let* b = get t @@ Node.Array.to_metakey node in
      let meta = Metadata.Array.decode b in
      let shape = Metadata.Array.shape meta in
      let slice_shape = match Indexing.slice_shape slice shape with
        | exception Assert_failure _ -> raise Invalid_array_slice
        | s -> s in
      if Ndarray.shape x <> slice_shape then raise Invalid_array_slice else
      let kind = Ndarray.data_type x in
      if not @@ Metadata.Array.is_valid_kind meta kind then raise Invalid_data_type else
      let m =
        Array.fold_left
          (fun acc (co, y) ->
            let k, c = Metadata.Array.index_coord_pair meta co in
            ArrayMap.add_to_list k (c, y) acc)
          ArrayMap.empty @@ Array.combine
            (Indexing.coords_of_slice slice shape) (Ndarray.to_array x)
      in
      let fill_value = Metadata.Array.fillvalue_of_kind meta kind in
      let repr = Codecs.{kind; shape = Metadata.Array.chunk_shape meta} in
      let prefix = Node.Array.to_key node ^ "/" in
      let chain = Metadata.Array.codecs meta in
      (* NOTE: there is opportunity to compute this step in parallel since
         each iteration acts on independent chunks. Maybe use Domainslib? *)
      let update_chunk (idx, pairs) =
        let ckey = prefix ^ Metadata.Array.chunk_key meta idx in
        if Io_chain.is_just_sharding chain then
          let* shardsize = size t ckey in
          let pget = get_partial_values t ckey in
          let pset = set_partial_values t ckey in
          Io_chain.partial_encode chain pget pset shardsize repr pairs fill_value
        else is_member t ckey >>= function
        | true ->
          let* v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr
        | false ->
          let arr = Ndarray.create repr.kind repr.shape fill_value in
          List.iter (fun (c, v) -> Ndarray.set arr c v) pairs;
          set t ckey @@ Codecs.Chain.encode chain arr
      in
      Deferred.iter update_chunk ArrayMap.(bindings m)

    let read :
      type a. t ->
      Node.Array.t ->
      Indexing.index array ->
      a Ndarray.dtype ->
      a Ndarray.t Deferred.t
      = fun t node slice kind ->
      let* b = get t @@ Node.Array.to_metakey node in
      let meta = Metadata.Array.decode b in
      if not @@ Metadata.Array.is_valid_kind meta kind
      then raise Invalid_data_type else
      let shape = Metadata.Array.shape meta in
      let slice_shape = match Indexing.slice_shape slice shape with
        | exception Assert_failure _ -> raise Invalid_array_slice
        | s -> s in
      let ic = Array.mapi (fun i v -> i, v) (Indexing.coords_of_slice slice shape) in
      let m =
        Array.fold_left
          (fun acc (i, y) ->
            let k, c = Metadata.Array.index_coord_pair meta y in
            ArrayMap.add_to_list k (i, c) acc) ArrayMap.empty ic in
      let chain = Metadata.Array.codecs meta in
      let prefix = Node.Array.to_key node ^ "/" in
      let fill_value = Metadata.Array.fillvalue_of_kind meta kind in
      let repr = Codecs.{kind; shape = Metadata.Array.chunk_shape meta} in
      (* NOTE: there is opportunity to compute this step in parallel since
         each iteration acts on independent chunks. *)
      (* `pairs` argument is a list of (C-order index of value within slice, coordinate within chunk) pairs.*)
      let read_chunk_data (idx, pairs) =
        let ckey = prefix ^ Metadata.Array.chunk_key meta idx in
        size t ckey >>= function
        | 0 -> Deferred.return @@ List.map (fun (i, _) -> i, fill_value) pairs
        | shardsize when Io_chain.is_just_sharding chain ->
          let pget = get_partial_values t ckey in
          Io_chain.partial_decode chain pget shardsize repr pairs fill_value
        | _ ->
          let+ v = get t ckey in
          let arr = Codecs.Chain.decode chain repr v in
          List.map (fun (i, c) -> i, Ndarray.get arr c) pairs
      in
      let+ pairs = Deferred.concat_map read_chunk_data ArrayMap.(bindings m) in
      (* sorting restores the C-order of the decoded array coordinates. *)
      let v =
        Array.of_list @@ snd @@ List.split @@
        List.fast_sort (fun (x, _) (y, _) -> Int.compare x y) pairs in
      Ndarray.of_array kind slice_shape v

    let reshape t node nshape =
      let mkey = Node.Array.to_metakey node in
      let* b = get t mkey in
      let meta = Metadata.Array.decode b in
      let oshape = Metadata.Array.shape meta in
      if Array.(length nshape <> length oshape)
      then raise Invalid_resize_shape else
      let s = ArraySet.of_list @@ Metadata.Array.chunk_indices meta oshape in
      let s' = ArraySet.of_list @@ Metadata.Array.chunk_indices meta nshape in
      let pre = Node.Array.to_key node ^ "/" in
      let* () = ArraySet.(elements @@ diff s s') |> Deferred.iter @@ fun v ->
        let key = pre ^ Metadata.Array.chunk_key meta v in
        is_member t key >>= function
        | true -> erase t key
        | false -> Deferred.return_unit
      in
      set t mkey @@ Metadata.Array.(encode @@ update_shape meta nshape)

    let rename t node str =
      let key = Node.Array.to_key node in
      exists t node >>= function
      | false -> raise @@ Key_not_found key
      | true -> rename t key Node.Array.(rename node str |> to_key)
  end
end
