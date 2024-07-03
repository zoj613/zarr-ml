open Metadata

type key = string

type range = ByteRange of int * int option

type error =
  [ `Store_read of string
  | `Store_write of string
  | Metadata.error
  | Codecs.error ]

module type STORE = sig
  (** The abstract STORE interface that stores should implement.

      The store interface defines a set of operations involving keys and values.
      In the context of this interface, a key is a Unicode string, where the final
      character is not a / character. In general, a value is a sequence of bytes.
      Specific stores may choose more specific storage formats, which must be
      stated in the specification of the respective store. 

      It is assumed that the store holds (key, value) pairs, with only one
      such pair for any given key. I.e., a store is a mapping from keys to
      values. It is also assumed that keys are case sensitive, i.e., the keys
      “foo” and “FOO” are different. The store interface also defines some
      operations involving prefixes. In the context of this interface,
      a prefix is a string containing only characters that are valid for use
      in keys and ending with a trailing / character. *)

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
  (** The storage type. *)

  val create_group
    : ?metadata:GroupMetadata.t -> t -> Node.t -> unit
  (** [create_group ~meta t node] creates a group node in store [t]
      containing metadata [meta]. This is a no-op if a node [node]
      is already a member of this store. *)

  val create_array
    : ?sep:Extensions.separator ->
      ?dimension_names:string option list ->
      ?attributes:Yojson.Safe.t ->
      ?codecs:Codecs.chain ->
      shape:int array ->
      chunks:int array ->
      ('a, 'b) Bigarray.kind ->
      'a ->
      Node.t ->
      t ->
      (unit, [> Codecs.error]) result
  (** [create_array ~sep ~dimension_names ~attributes ~codecs ~shape ~chunks kind fill node t]
      creates an array node in store [t] where:
      - Separator [sep] is used in the array's chunk key encoding.
      - Dimension names [dimension_names] and user attributes [attributes]
        are included in it's metadata document.
      - A codec chain defined by [codecs].
      - The array has shape [shape] and chunk shape [chunks].
      - The array has data kind [kind] and fill value [fv].
      
      This operation can fail if the codec chain is not well defined. *)

  val array_metadata
    : Node.t -> t -> (ArrayMetadata.t, [> error]) result
  (** [array_metadata node t] returns the metadata of array node [node].
      This operation returns an error if:
      - The node is not a member of store [t].
      - if node [node] is a group node. *)

  val group_metadata
    : Node.t -> t -> (GroupMetadata.t, [> error]) result
  (** [group_metadata node t] returns the metadata of group node [node].
      This operation returns an error if:
      - The node is not a member of store [t].
      - if node [node] is an array node. *)

  val find_child_nodes
    : t -> Node.t -> (Node.t list * Node.t list, [> error]) result
  (** [find_child_nodes t n] returns a tuple of child nodes of group node [n].
      The first element of the tuple is a list of array child nodes, and the
      second element a list of child group nodes.
      This operation can fail if:
      - Node [n] is not a member of store [t].
      - Node [n] is an array node of store [t]. *)

  val find_all_nodes : t -> Node.t list
  (** [find_all_nodes t] returns a list of all nodes in store [t]. If the
      store has no nodes, an empty list is returned. *)

  val erase_node : t -> Node.t -> unit
  (** [erase_node t n] erases node [n] from store [t]. This function erases
      all child nodes if [n] is a group node. If node [n] is not a member
      of store [t] then this is a no-op. *)

  val is_member : t -> Node.t -> bool
  (** [is_member t n] returns [true] if node [n] is a member of store [t]
      and [false] otherwise. *)
    
  val set_array
    : Node.t ->
      Owl_types.index array ->
      ('a, 'b) Ndarray.t ->
      t ->
      (unit, [> error]) result
  (** [set_array n s x t] writes n-dimensional array [x] to the slice [s]
      of array node [n] in store [t]. This operation fails if:
      - the ndarray [x] size does not equal slice [s].
      - the kind of [x] is not compatible with node [n]'s data type as
        described in its metadata document.
      - If there is a problem decoding/encoding node [n] chunks.*)

  val get_array
    : Node.t ->
      Owl_types.index array ->
      ('a, 'b) Bigarray.kind ->
      t ->
      (('a, 'b) Ndarray.t, [> error]) result
  (** [get_array n s k t] reads an n-dimensional array of size determined
      by slice [s] from array node [n]. This operation fails if:
      - If there is a problem decoding/encoding node [n] chunks.
      - kind [k] is not compatible with node [n]'s data type as described
        in its metadata document.
      - The slice [s] is not a valid slice of array node [n].*)

  val reshape : t -> Node.t -> int array -> (unit, [> error]) result
  (** [reshape t n shape] resizes array node [n] of store [t] into new
      size [shape]. If this operation fails, an error is returned. It
      can fail if:
      - Node [n] is not a valid array node.
      - If [shape] does not have the same dimensions as node [n]'s shape. *)
end

module type MAKER = functor (M : STORE) -> S with type t = M.t

module type Interface = sig
  (** A Zarr store is a system that can be used to store and retrieve data
   * from a Zarr hierarchy. For a store to be compatible with this
   * specification, it must support a set of operations defined in the
   * Abstract store interface {!STORE}. The store interface can be
   * implemented using a variety of underlying storage technologies. *)

  type error
  (** The error type of supported storage backends. *)

  module type S = S
  (** The public interface of all supported stores. *)

  module type STORE = STORE
  (** The module interface that all supported stores must implement. *)

  module type MAKER = MAKER

  module Make : MAKER
  (** A functor for minting a new storage type as long as it's argument
      module implements the {!STORE} interface. *)
end

module Base = struct
  (** general implementation agnostic STORE interface functions.
   * To be used as fallback functions for stores that do not
   * readily provide implementations for these functions. *) 

  module StrSet = Set.Make (String)

  let erase_values ~erase_fn t keys =
    StrSet.iter (erase_fn t) @@ StrSet.of_list keys

  let erase_prefix ~list_fn ~erase_fn t pre =
    List.iter (fun k ->
      if String.starts_with ~prefix:pre k
      then begin
        erase_fn t k
      end) @@ list_fn t

  let list_prefix ~list_fn t pre =
    List.filter
      (String.starts_with ~prefix:pre) 
      (list_fn t)

  let list_dir ~list_fn t pre =
    let paths =
      List.map
        (fun k ->
          Result.get_ok @@
          Node.of_path @@
          String.cat "/" k)
        (list_prefix ~list_fn t pre)
    in
    let is_prefix_child k =
      match Node.parent k with
      | Some par ->
        String.equal pre @@ Node.to_prefix par
      | None -> false in
    let keys, rest =
      List.partition_map (fun k ->
        match is_prefix_child k with
        | true -> Either.left @@ Node.to_key k
        | false -> Either.right k)
      paths
    in
    let prefixes =
      List.fold_left (fun acc k ->
        match
          List.find_opt
            is_prefix_child
            (Node.ancestors k)
        with
        | None -> acc
        | Some v ->
          let w = Node.to_prefix v in
          if List.mem w acc then acc
          else w :: acc)
      [] rest
    in
    keys, prefixes

  let rec get_partial_values ~get_fn t kr_pairs =
    match kr_pairs with
    | [] -> [None]
    | (k, r) :: xs ->
      match get_fn t k with
      | Error _ ->
        None :: (get_partial_values ~get_fn t xs)
      | Ok v ->
        try
          let sub = match r with
            | ByteRange (rs, None) ->
              String.sub v rs @@ String.length v 
            | ByteRange (rs, Some rl) ->
              String.sub v rs rl in
          Some sub :: (get_partial_values ~get_fn t xs)
        with
        | Invalid_argument _ ->
          None :: (get_partial_values ~get_fn t xs)
    
  let rec set_partial_values ~set_fn ~get_fn t = function
    | [] -> Ok ()
    | (k, rs, v) :: xs ->
      match get_fn t k with
      | Error _ ->
        set_fn t k v;
        set_partial_values ~set_fn ~get_fn t xs
      | Ok ov ->
        try
          let ov' = Bytes.of_string ov in
          String.(length v |> blit v 0 ov' rs);
          set_fn t k @@ Bytes.to_string ov';
          set_partial_values ~set_fn ~get_fn t xs
        with
        | Invalid_argument s ->
          Error (`Store_read s)
end
