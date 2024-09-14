open Metadata
open Node

exception Invalid_resize_shape
exception Invalid_data_type
exception Invalid_array_slice
exception Key_not_found of string
exception Not_a_filesystem_store of string

module type STORE = sig
  module Deferred : Types.Deferred
  type t
  (** The storage type. *)

  val create_group : ?attrs:Yojson.Safe.t -> t -> GroupNode.t -> unit Deferred.t
  (** [create_group ?attrs t node] creates a group node in store [t]
      containing attributes [attrs]. This is a no-op if [node]
      is already a member of this store. *)

  val create_array :
    ?sep:[< `Dot | `Slash > `Slash ] ->
    ?dimension_names:string option list ->
    ?attributes:Yojson.Safe.t ->
    codecs:Codecs.codec_chain ->
    shape:int array ->
    chunks:int array ->
    'a Ndarray.dtype ->
    'a ->
    ArrayNode.t ->
    t ->
    unit Deferred.t
  (** [create_array ~sep ~dimension_names ~attributes ~codecs ~shape ~chunks kind fill node t]
      creates an array node in store [t] where:
      - Separator [sep] is used in the array's chunk key encoding.
      - Dimension names [dimension_names] and user attributes [attributes]
        are included in it's metadata document.
      - A codec chain defined by [codecs].
      - The array has shape [shape] and chunk shape [chunks].
      - The array has data kind [kind] and fill value [fv].
      
      @raise Codecs.Bytes_to_bytes_invariant
        if [codecs] contains more than one bytes->bytes codec.
      @raise Codecs.Invalid_transpose_order
        if [codecs] contains a transpose codec with invalid order array.
      @raise Codecs.Invalid_sharding_chunk_shape
        if [codecs] contains a shardingindexed codec with an
        incorrect inner chunk shape. *)

  val array_metadata : t -> ArrayNode.t -> ArrayMetadata.t Deferred.t
  (** [array_metadata node t] returns the metadata of array node [node].

      @raise Key_not_found if node is not a member of store [t]. *)

  val group_metadata : t -> GroupNode.t -> GroupMetadata.t Deferred.t
  (** [group_metadata node t] returns the metadata of group node [node].

      @raise Key_not_found if node is not a member of store [t].*)

  val find_child_nodes
    : t -> GroupNode.t -> (ArrayNode.t list * GroupNode.t list) Deferred.t
  (** [find_child_nodes t n] returns a tuple of child nodes of group node [n].
      This operation returns a pair of empty lists if node [n] has no
      children or is not a member of store [t].
      
      @raise Parse_error if any child node has invalid [node_type] metadata.*)

  val find_all_nodes : t -> (ArrayNode.t list * GroupNode.t list) Deferred.t
  (** [find_all_nodes t] returns [Some p] where [p] is a pair of lists
      representing all nodes in store [t]. The first element of the pair
      is a list of all array nodes, and the second element is a list of
      all group nodes. This operation returns a pair of empty lists if
      store [t] is empty.

      @raise Parse_error if any node has invalid [node_type] metadata.*)

  val erase_group_node : t -> GroupNode.t -> unit Deferred.t
  (** [erase_group_node t n] erases group node [n] from store [t]. This also
      erases all child nodes of [n]. If node [n] is not a member
      of store [t] then this is a no-op. *)

  val erase_array_node : t -> ArrayNode.t -> unit Deferred.t
  (** [erase_array_node t n] erases group node [n] from store [t]. This also
      erases all child nodes of [n]. If node [n] is not a member
      of store [t] then this is a no-op. *)

  val erase_all_nodes : t -> unit Deferred.t
  (** [erase_all_nodes t] clears the store [t] by deleting all nodes.
      If the store is already empty, this is a no-op. *)

  val group_exists : t -> GroupNode.t -> bool Deferred.t
  (** [group_exists t n] returns [true] if group node [n] is a member
      of store [t] and [false] otherwise. *)
    
  val array_exists : t -> ArrayNode.t -> bool Deferred.t
  (** [array_exists t n] returns [true] if array node [n] is a member
      of store [t] and [false] otherwise. *)

  val write_array :
    t ->
    ArrayNode.t ->
    Ndarray.Indexing.index array ->
    'a Ndarray.t ->
    unit Deferred.t
  (** [write_array t n s x] writes n-dimensional array [x] to the slice [s]
      of array node [n] in store [t].

      @raise Invalid_array_slice
        if the ndarray [x] size does not equal slice [s].
      @raise Invalid_data_type
        if the kind of [x] is not compatible with node [n]'s data type as
          described in its metadata document. *)

  val read_array :
    t ->
    ArrayNode.t ->
    Ndarray.Indexing.index array ->
    'a Ndarray.dtype ->
    'a Ndarray.t Deferred.t
  (** [read_array t n s k] reads an n-dimensional array of size determined
      by slice [s] from array node [n].

      @raise Invalid_data_type
        if kind [k] is not compatible with node [n]'s data type as described
          in its metadata document.
      @raise Invalid_array_slice
        if the slice [s] is not a valid slice of array node [n].*)

  val reshape : t -> ArrayNode.t -> int array -> unit Deferred.t
  (** [reshape t n shape] resizes array node [n] of store [t] into new
      size [shape].

      @raise Invalid_resize_shape
        if [shape] does not have the same dimensions as [n]'s shape.
      @raise Key_not_found
        if node [n] is not a member of store [t]. *)

  val rename_group : t -> GroupNode.t -> string -> unit Deferred.t
  (** [rename t g name] changes the name of group node [g] in store [t] to [name].

      @raise Key_not_found if [g] is not a member of store [t].
      @raise Renaming_root if [g] is the store's root node.
      @raise Node_invariant if [name] is an invalid node name.*)

  val rename_array : t -> ArrayNode.t -> string -> unit Deferred.t
  (** [rename t n name] changes the name of array node [n] in store [t] to [name].

      @raise Key_not_found if [g] is not a member of store [t].
      @raise Renaming_root if [g] is the store's root node.
      @raise Node_invariant if [name] is an invalid node name.*)
end

module type Interface = sig
  (** A Zarr store is a system that can be used to store and retrieve data
      from a Zarr hierarchy. For a store to be compatible with this
      specification, it must support a set of operations defined in the
      Abstract store interface {!STORE}. The store interface can be
      implemented using a variety of underlying storage technologies. *)

  exception Invalid_resize_shape
  (** raised when resizing a Zarr array with an incorrect shape. *)

  exception Invalid_data_type 
  (** raised when supplied data type is not the same as Zarr array's. *)

  exception Invalid_array_slice
  (** raised when requesting a view of a Zarr array with an incorrect slice. *)
  
  exception Key_not_found of string
  (** raised when a node's chunk key or metadata key is found in a store. *)

  exception Not_a_filesystem_store of string
  (** raised when opening a file that as if it was a Filesystem Zarr store. *)

  module type STORE = STORE
  (** The module interface that all supported stores must implement. *)

  module Make : functor (Io : Types.IO) -> STORE
    with type t = Io.t and type 'a Deferred.t = 'a Io.Deferred.t
  (** A functor for minting a new storage type as long as it's argument
      module implements the {!STORE} interface. *)
end
