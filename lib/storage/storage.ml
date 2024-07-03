include Storage_intf

open Util.Result_syntax

module Make (M : STORE) : S with type t = M.t = struct
  module Ndarray = Owl.Dense.Ndarray.Generic
  module ArraySet = Util.ArraySet
  module Arraytbl = Util.Arraytbl
  module AM = Metadata.ArrayMetadata
  module GM = Metadata.GroupMetadata
  include M

  (* All nodes are explicit upon creation so just check the node's metadata key.*)
  let is_member t node =
    M.is_member t @@ Node.to_metakey node

  let rec create_group ?metadata t node =
    if is_member t node then ()
    else
      (match metadata, Node.to_metakey node with
      | Some m, k -> set t k @@ GM.encode m;
      | None, k -> set t k @@ GM.(default |> encode));
      make_implicit_groups_explicit t @@ Node.parent node

  and make_implicit_groups_explicit t = function
    | None -> ()
    | Some n -> create_group t n

  let create_array
    ?(sep=Extensions.Slash)
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
    let meta =
      AM.create
        ~sep
        ~codecs
        ~dimension_names
        ~attributes
        ~shape
        kind
        fill_value
        chunks
    in
    set t (Node.to_metakey node) (AM.encode meta);
    Ok (make_implicit_groups_explicit t @@ Node.parent node)

  (* Assumes without checking that [metakey] is a valid node metadata key.*)
  let unsafe_node_type t metakey =
    let open Yojson.Safe in
    get t metakey |> Result.get_ok |> from_string
    |> Util.member "node_type" |> Util.to_string

  let group_metadata node t =
    if not @@ is_member t node then
      Result.error @@
      `Store_read (Node.show node ^ " is not a member of this store.")
    else
      get t @@ Node.to_metakey node >>= fun bytes ->
      match GM.decode bytes with
      | Ok meta -> Ok meta
      | Error _ ->
        Result.error @@
        `Store_read (Node.show node ^ " is not an array node.")

  let array_metadata node t =
    if not @@ is_member t node then
      Result.error @@
      `Store_read (Node.show node ^ " is not a member of this store.")
    else
      get t @@ Node.to_metakey node >>= fun bytes ->
      match AM.decode bytes with
      | Ok meta -> Ok meta
      | Error _ ->
        Result.error @@
        `Store_read (Node.show node ^ " is not an array node.")

  let find_child_nodes t node =
    match is_member t node, Node.to_metakey node with
    | true, k when unsafe_node_type t k = "group" ->
      Result.ok @@
      List.fold_left
        (fun (lacc, racc) pre ->
          let x =
            Result.get_ok @@  (* this operation should not fail *)
            Node.of_path @@
            "/" ^ String.(length pre - 1 |> sub pre 0)
          in
          if unsafe_node_type t (pre ^ "zarr.json") = "array" then
            x :: lacc, racc
          else
            lacc, x :: racc)
        ([], []) (snd @@ list_dir t @@ Node.to_prefix node)
    | true, _ ->
      Result.error @@
      `Store_read (Node.show node ^ " is not a group node.")
    | false, _ ->
      Result.error @@
      `Store_read (Node.show node ^ " is not a node in this heirarchy.")

  let find_all_nodes t =
    let rec aux acc p =
      match find_child_nodes t p with
      | Error _ -> acc
      | Ok ([], []) -> p :: acc
      | Ok (arrays, groups) ->
        arrays @ p :: List.concat_map (aux acc) groups
    in aux [] Node.root

  let erase_node t node =
    erase_prefix t @@ Node.to_prefix node

  let set_array
  : type a b.
    Node.t ->
    Owl_types.index array ->
    (a, b) Ndarray.t ->
    t ->
    (unit, [> error]) result
  = fun node slice x t ->
    let open Util in
    get t @@ Node.to_metakey node >>= fun bytes ->
    AM.decode bytes >>= fun meta ->
    (if Ndarray.shape x = Indexing.slice_shape slice @@ AM.shape meta then 
        Ok ()
      else
        Error (`Store_write "slice and input array shapes are unequal."))
    >>= fun () ->
    (if AM.is_valid_kind meta @@ Ndarray.kind x then
        Ok ()
      else
       Result.error @@
       `Store_write (
         "input array's kind is not compatible with node's data type."))
    >>= fun () ->
    let coords = Indexing.coords_of_slice slice @@ AM.shape meta in
    let tbl = Arraytbl.create @@ Array.length coords
    in
    Ndarray.iteri (fun i y ->
      let k, c = AM.index_coord_pair meta coords.(i) in
      Arraytbl.add tbl k (c, y)) x;
    let repr =
      {kind = Ndarray.kind x
      ;shape = AM.chunk_shape meta
      ;fill_value = AM.fillvalue_of_kind meta @@ Ndarray.kind x}
    in
    let codecs = AM.codecs meta in
    let prefix = Node.to_prefix node in
    let cindices = ArraySet.of_seq @@ Arraytbl.to_seq_keys tbl in
    ArraySet.fold (fun idx acc ->
      acc >>= fun () ->
      let chunkkey = prefix ^ AM.chunk_key meta idx in
      (match get t chunkkey with
      | Ok b ->
        Codecs.Chain.decode codecs repr b
      | Error _ ->
        Ok (Ndarray.create repr.kind repr.shape repr.fill_value))
      >>= fun arr ->
      (* find_all returns bindings in reverse order. To restore the
       * C-ordering of elements we must call List.rev. *)
      let coords, vals =
        List.split @@
        List.rev @@
        Arraytbl.find_all tbl idx in
      let slice' = Indexing.slice_of_coords coords in
      let shape' = Indexing.slice_shape slice' repr.shape in
      let x' = Ndarray.of_array repr.kind (Array.of_list vals) shape' in
      (* Ndarray.set_fancy* unfortunately doesn't work for array kinds
         other than Float32, Float64, Complex32 and Complex64.
         See: https://github.com/owlbarn/owl/issues/671 *)
      Ndarray.set_fancy_ext slice' arr x'; (* possible to rewrite this function? *)
      Codecs.Chain.encode codecs arr >>| fun encoded ->
      set t chunkkey encoded) cindices (Ok ())

  let get_array
  : type a b.
    Node.t ->
    Owl_types.index array ->
    (a, b) Bigarray.kind ->
    t ->
    ((a, b) Ndarray.t, [> error]) result
  = fun node slice kind t ->
    let open Util in
    get t @@ Node.to_metakey node >>= fun bytes ->
    AM.decode bytes >>= fun meta ->
    (if AM.is_valid_kind meta kind then
        Ok ()
      else
       Result.error @@
       `Store_read ("input kind is not compatible with node's data type."))
    >>= fun () ->
    (try
      Ok (Indexing.slice_shape slice @@ AM.shape meta)
    with
    | Assert_failure _ -> 
      Result.error @@
      `Store_read "slice shape is not compatible with node's shape.")
    >>= fun sshape ->
    let pair = 
      Array.map
        (AM.index_coord_pair meta)
        (Indexing.coords_of_slice slice @@ AM.shape meta) in
    let tbl = Arraytbl.create @@ Array.length pair in
    let prefix = Node.to_prefix node in
    let chain = AM.codecs meta in
    let repr =
      {kind
      ;shape = AM.chunk_shape meta
      ;fill_value = AM.fillvalue_of_kind meta kind}
    in
    Array.fold_right (fun (idx, coord) acc ->
      acc >>= fun l ->
      match Arraytbl.find_opt tbl idx with
      | Some arr ->
        Ok (Ndarray.get arr coord :: l)
      | None ->
        (match get t @@ prefix ^ AM.chunk_key meta idx with
        | Ok b ->
          Codecs.Chain.decode chain repr b
        | Error _ ->
          Ok (Ndarray.create repr.kind repr.shape repr.fill_value))
        >>= fun arr ->
        Arraytbl.add tbl idx arr;
        Ok (Ndarray.get arr coord :: l)) pair (Ok [])
    >>| fun res ->
    Ndarray.of_array kind (Array.of_list res) sshape

  let reshape t node shape =
    let mkey = Node.to_metakey node in
    (if "array" = unsafe_node_type t mkey then
      Ok ()
    else
      Error (`Store_write (Node.show node ^ " is not an array node.")))
    >>= fun () ->
    get t mkey >>= fun bytes ->
    AM.decode bytes >>= fun meta ->
    (if Array.length shape = Array.length @@ AM.shape meta then
      Ok ()
    else
      Error (`Store_write "new shape must have same number of dimensions."))
    >>= fun () ->
    let pre = Node.to_prefix node in
    let s =
      ArraySet.of_list @@ AM.chunk_indices meta @@ AM.shape meta in
    let s' =
      ArraySet.of_list @@ AM.chunk_indices meta shape in
    ArraySet.iter
      (fun v -> erase t @@ pre ^ AM.chunk_key meta v)
      ArraySet.(diff s s');
    Ok (set t mkey @@ AM.encode @@ AM.update_shape meta shape)
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
