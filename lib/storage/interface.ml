open Metadata
open Util.Result_syntax

type key = string
type range = ByteRange of int * int option

type error =
  [ `Store_read_error of string
  | `Invalid_slice of string
  | `Invalid_kind of string
  | `Reshape_error of string
  | `Invalid_byte_range of string
  | Codecs.error
  | Metadata.error ]

module type STORE = sig
  type t
  val get : t -> key -> (string, [> error]) result
  val get_partial_values : t -> (key * range) list -> string option list
  val set : t -> key -> string -> unit
  val set_partial_values : t -> (key * int * string) list -> (unit, [> error]) result
  val erase : t -> key -> unit
  val erase_values : t -> key list -> unit
  val erase_prefix : t -> key -> unit
  val list : t -> key list
  val list_prefix : key -> t -> key list
  val list_dir : t -> key -> key list * string list 
  val is_member : t -> key -> bool
end

module Ndarray = Owl.Dense.Ndarray.Generic

module type S = sig
  type t

  val create_group
    : ?metadata:GroupMetadata.t -> t -> Node.t -> unit

  val create_array
    : ?sep:Extensions.separator ->
      ?codecs:Codecs.chain ->
      shape:int array ->
      chunks:int array ->
      ('a, 'b) Bigarray.kind ->
      'a ->
      Node.t ->
      t ->
      (unit, [> error]) result

  val array_metadata
    : Node.t -> t -> (ArrayMetadata.t, [> error]) result

  val group_metadata
    : Node.t -> t -> (GroupMetadata.t, [> error]) result

  val find_child_nodes
    : t -> Node.t -> (Node.t list * Node.t list, string) result

  val find_all_nodes : t -> Node.t list

  val erase_node : t -> Node.t -> unit

  val is_member : t -> Node.t -> bool
    
  val set_array
    : Node.t ->
      Owl_types.index array ->
      ('a, 'b) Ndarray.t ->
      t ->
      (unit, [> error]) result

  val get_array
    : Node.t ->
      Owl_types.index array ->
      ('a, 'b) Bigarray.kind ->
      t ->
      (('a, 'b) Ndarray.t, [> error]) result

  val reshape
    : t -> Node.t -> int array -> (unit, [> error]) result
end

module Make (M : STORE) : S with type t = M.t = struct
  module ArraySet = Util.ArraySet
  module Arraytbl = Util.Arraytbl
  module AM = ArrayMetadata
  module GM = GroupMetadata
  include M

  let rec create_group ?metadata t node =
    match metadata, Node.to_metakey node with
    | Some m, k -> set t k @@ GM.encode m;
    | None, k -> set t k @@ GM.(default |> encode);
    make_implicit_groups_explicit t node

  and make_implicit_groups_explicit t node =
    List.iter (fun n ->
      match get t @@ Node.to_metakey n with
      | Ok _ -> ()
      | Error _ -> create_group t n) @@ Node.ancestors node

  let create_array
    ?(sep=Extensions.Slash) ?codecs ~shape ~chunks kind fill_value node t =
    let open Util in
    let repr =
      {kind
      ;fill_value
      ;shape = chunks}
    in
    (match codecs with
    | Some c -> Codecs.Chain.create repr c
    | None -> Ok Codecs.Chain.default)
    >>= fun codecs' ->
    let meta =
      AM.create ~sep ~codecs:codecs' ~shape kind fill_value chunks in
    set t (Node.to_metakey node) (AM.encode meta);
    Ok (make_implicit_groups_explicit t node)

  (* All nodes are explicit upon creation so just check the node's metadata key.*)
  let is_member t node =
    M.is_member t @@ Node.to_metakey node

  (* Assumes without checking that [metakey] is a valid node metadata key.*)
  let unsafe_node_type t metakey =
    let open Yojson.Safe in
    get t metakey |> Result.get_ok |> from_string
    |> Util.member "node_type" |> Util.to_string

  let get_metadata node t =
    match is_member t node, Node.to_metakey node with
    | true, k when unsafe_node_type t k = "array" ->
      get t k >>= fun bytes ->
      AM.decode bytes >>= fun meta ->
      Ok (Either.left meta)
    | true, k ->
      get t k >>= fun bytes ->
      GM.decode bytes >>= fun meta ->
      Ok (Either.right meta)
    | false, _ ->
      Error (`Store_read_error (Node.to_path node ^ " is not a store member."))

  let group_metadata node t =
    match get_metadata node t with
    | Ok x -> Ok (Either.find_right x |> Option.get)
    | Error _ as err -> err

  let array_metadata node t =
    match get_metadata node t with
    | Ok x -> Ok (Either.find_left x |> Option.get)
    | Error _ as err -> err

  let find_child_nodes t node =
    match is_member t node, Node.to_metakey node with
    | true, k when unsafe_node_type t k = "group" ->
      Result.ok @@
      List.fold_left (fun (lacc, racc) pre ->
        match
          Node.of_path @@
          "/" ^ String.(length pre - 1 |> sub pre 0)
        with
        | Ok x ->
          if unsafe_node_type t (pre ^ "zarr.json") = "array" then
            x :: lacc, racc
          else
            lacc, x :: racc
        | Error _ -> lacc, racc)
        ([], []) (snd @@ list_dir t @@ Node.to_prefix node)
    | true, _ ->
      Error (Node.to_path node ^ " is not a group node.")
    | false, _ ->
      Error (Node.to_path node ^ " is not a node in this heirarchy.")

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
        Error (`Invalid_slice "slice and input array shapes are unequal."))
    >>= fun () ->
    (if AM.is_valid_kind meta @@ Ndarray.kind x then
        Ok ()
      else
       Result.error @@
       `Invalid_kind (
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
       `Invalid_kind ("input kind is not compatible with node's data type."))
    >>= fun () ->
    (try
      Ok (Indexing.slice_shape slice @@ AM.shape meta)
    with
    | Assert_failure _ -> 
      Result.error @@
      `Store_read_error "slice shape is not compatible with node's shape.")
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
      Error (`Reshape_error (Node.to_path node ^ " is not an array node.")))
    >>= fun () ->
    get t mkey >>= fun bytes ->
    AM.decode bytes >>= fun meta ->
    (if Array.length shape = Array.length @@ AM.shape meta then
      Ok ()
    else
      Error (`Reshape_error "new shape must have same number of dimensions."))
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
