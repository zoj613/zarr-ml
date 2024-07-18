open Metadata
open Node

type key = string

type range = int * int option

type error =
  [ `Store_read of string
  | `Store_write of string ]

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
  val size : t -> key -> int
  val get : t -> key -> (string, [> `Store_read of string ]) result
  val get_partial_values :
    t -> key -> range list -> (string list, [> `Store_read of string ]) result
  val set : t -> key -> string -> unit
  val set_partial_values : t -> key -> ?append:bool -> (int * string) list -> unit
  val erase : t -> key -> unit
  val erase_values : t -> key list -> unit
  val erase_prefix : t -> key -> unit
  val list : t -> key list
  val list_prefix : key -> t -> key list
  val list_dir : t -> key -> key list * string list 
  val is_member : t -> key -> bool
end

module type S = sig
  type t
  (** The storage type. *)

  val create_group
    : ?metadata:GroupMetadata.t -> t -> GroupNode.t -> unit
  (** [create_group ~meta t node] creates a group node in store [t]
      containing metadata [meta]. This is a no-op if [node]
      is already a member of this store. *)

  val create_array
    : ?sep:[< `Dot | `Slash > `Slash ] ->
      ?dimension_names:string option list ->
      ?attributes:Yojson.Safe.t ->
      ?codecs:Codecs.codec_chain ->
      shape:int array ->
      chunks:int array ->
      ('a, 'b) Bigarray.kind ->
      'a ->
      ArrayNode.t ->
      t ->
      (unit, [> Codecs.error | Extensions.error | Metadata.error ]) result
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
    : ArrayNode.t -> t -> (ArrayMetadata.t, [> error ]) result
  (** [array_metadata node t] returns the metadata of array node [node].
      This operation returns an error if node is not a member of store [t]. *)

  val group_metadata
    : GroupNode.t -> t -> (GroupMetadata.t, [> error ]) result
  (** [group_metadata node t] returns the metadata of group node [node].
      This operation returns an error if node is not a member of store [t].*)

  val find_child_nodes
    : t -> GroupNode.t -> ArrayNode.t list * GroupNode.t list
  (** [find_child_nodes t n] returns a tuple of child nodes of group node [n].
      This operation returns a pair of empty lists if node [n] has no
      children or is not a member of store [t]. *)

  val find_all_nodes : t -> ArrayNode.t list * GroupNode.t list
  (** [find_all_nodes t] returns [Some p] where [p] is a pair of lists
      representing all nodes in store [t]. The first element of the pair
      is a list of all array nodes, and the second element is a list of
      all group nodes. If the store has no nodes, [None] is returned. *)

  val erase_group_node : t -> GroupNode.t -> unit
  (** [erase_group_node t n] erases group node [n] from store [t]. This also
      erases all child nodes of [n]. If node [n] is not a member
      of store [t] then this is a no-op. *)

  val erase_array_node : t -> ArrayNode.t -> unit
  (** [erase_array_node t n] erases group node [n] from store [t]. This also
      erases all child nodes of [n]. If node [n] is not a member
      of store [t] then this is a no-op. *)

  val erase_all_nodes : t -> unit
  (** [erase_all_nodes t] clears the store [t] by deleting all nodes.
      If the store is already empty, this is a no-op. *)

  val group_exists : t -> GroupNode.t -> bool
  (** [group_exists t n] returns [true] if group node [n] is a member
      of store [t] and [false] otherwise. *)
    
  val array_exists : t -> ArrayNode.t -> bool
  (** [array_exists t n] returns [true] if array node [n] is a member
      of store [t] and [false] otherwise. *)

  val set_array
    : ArrayNode.t ->
      Owl_types.index array ->
      ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
      t ->
      (unit, [> error | Node.error | Codecs.error ]) result
  (** [set_array n s x t] writes n-dimensional array [x] to the slice [s]
      of array node [n] in store [t]. This operation fails if:
      - the ndarray [x] size does not equal slice [s].
      - the kind of [x] is not compatible with node [n]'s data type as
        described in its metadata document.
      - If there is a problem decoding/encoding node [n] chunks.*)

  val get_array
    : ArrayNode.t ->
      Owl_types.index array ->
      ('a, 'b) Bigarray.kind ->
      t ->
      (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
      ,[> error | Node.error | Codecs.error ]) result
  (** [get_array n s k t] reads an n-dimensional array of size determined
      by slice [s] from array node [n]. This operation fails if:
      - If there is a problem decoding/encoding node [n] chunks.
      - kind [k] is not compatible with node [n]'s data type as described
        in its metadata document.
      - The slice [s] is not a valid slice of array node [n].*)

  val reshape : t -> ArrayNode.t -> int array -> (unit, [> error]) result
  (** [reshape t n shape] resizes array node [n] of store [t] into new
      size [shape]. If this operation fails, an error is returned.
      It can fail if [shape] does not have the same dimensions as [n]'s shape.
      If node [n] is not a member of store [t] then this is a no-op. *)
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

  let list_prefix ~list_fn t pre =
    List.filter
      (String.starts_with ~prefix:pre) 
      (list_fn t)

  let erase_values ~erase_fn t keys =
    StrSet.iter (erase_fn t) @@ StrSet.of_list keys

  let erase_prefix ~list_fn ~erase_fn t pre =
    erase_values ~erase_fn t @@ list_prefix ~list_fn t pre

  let list_dir ~list_fn t pre =
    let n = String.length pre in
    let keys, rest =
      StrSet.fold
      (fun k (l, r) ->
        if not @@ String.contains_from k n '/' then 
          StrSet.add k l, r
        else
          l, StrSet.add k r)
      (StrSet.of_list @@ list_prefix ~list_fn t pre)
      (StrSet.empty, StrSet.empty)
    in
    let prefixes =
      StrSet.map
        (fun k ->
          String.sub k 0 @@
          1 + String.index_from k n '/') rest
    in
    StrSet.(elements keys, elements prefixes)

  let get_partial_values ~get_fn t key ranges =
    let open Util.Result_syntax in
    List.fold_right
      (fun (rs, len) acc ->
        acc >>= fun xs ->
        get_fn t key >>| fun v ->
        (match len with
        | None -> String.sub v rs @@ String.length v - rs
        | Some l -> String.sub v rs l) :: xs) ranges (Ok [])

  let set_partial_values ~set_fn ~get_fn t key append rv =
    List.iter
      (fun (rs, v) ->
        let ov = get_fn t key |> Result.get_ok in
        if append then
          let ov' = ov ^ v in
          set_fn t key ov'
        else
          let ov' = Bytes.of_string ov in
          String.(length v |> Bytes.blit_string v 0 ov' rs);
          set_fn t key @@ Bytes.to_string ov') rv
end
