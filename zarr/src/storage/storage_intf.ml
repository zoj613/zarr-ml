module type S = sig
  type t
  (** The storage type. *)

  type 'a io
  (** The I/O monad type.*)

  type error
  (** The store error type. *)

  val hierarchy :
    t ->
    (Node.Array.t list * Node.Group.t list,
     [> `Zarr of error
     | `Parse_error of string
     | `Invalid_path of string
     | `Node_invariant of string ])
    result io
  (** [hierarchy t] returns [p] where [p] is a pair of lists
      representing all nodes in store [t]. The first element of the pair
      is a list of all array nodes, and the second element is a list of
      all group nodes. This operation returns a pair of empty lists if
      store [t] is empty. *)

  val clear : t -> (unit, [> `Zarr of error]) result io
  (** [clear t] clears the store [t] by deleting all nodes.
      If the store is already empty, this is a no-op. *)

  module Group : sig
    val create :
      ?attrs:Yojson.Safe.t ->
      t ->
      Node.Group.t ->
      (unit, [> `Zarr of error]) result io
    (** [create ?attrs t node] creates a group node in store [t]
        containing attributes [attrs]. This is a no-op if [node]
        is already a member of this store. *)

    val metadata :
      t ->
      Node.Group.t ->
      (Metadata.Group.t, [> `Zarr of error | `Parse_error of string ]) result io
    (** [metadata node t] returns the metadata of group node [node].*)

    val children :
      t ->
      Node.Group.t ->
      (Node.Array.t list * Node.Group.t list,
       [> `Zarr of error
       | `Parse_error of string
       | `Invalid_path of string
       | `Node_invariant of string ])
      result io
    (** [children t n] returns a tuple of child nodes of group node [n].
        This operation returns a pair of empty lists if node [n] has no
        children or is not a member of store [t]. *)

    val delete : t -> Node.Group.t -> (unit, [> `Zarr of error]) result io
    (** [delete t n] erases group node [n] from store [t]. This also
        erases all child nodes of [n]. If node [n] is not a member
        of store [t] then this is a no-op. *)

    val exists : t -> Node.Group.t -> (bool, [> `Zarr of error]) result io
    (** [exists t n] returns [true] if group node [n] is a member
        of store [t] and [false] otherwise. *)

    val rename :
      t ->
      Node.Group.t ->
      string ->
      (Node.Group.t,
       [> `Zarr of error
       | `Key_not_found of string
       | `Node_invariant of string
       | `Cannot_rename_root ])
      result io
    (** [rename t g name] changes the name of group node [g] in store [t] to [name].*)
  end

  module Array : sig
    val create :
      ?overwrite:bool ->
      ?sep:[< `Dot | `Slash > `Slash ] ->
      ?attributes:Yojson.Safe.t ->
      ?dimension_names:string option list ->
      codecs:Codecs.codec list ->
      shape:int list ->
      chunks:int list ->
      'a Ndarray.dtype ->
      'a ->
      Node.Array.t ->
      t ->
      (unit,
       [> `Zarr of error
       | `Invalid_dimension_names
       | `Invalid_grid_chunk_shape
       | `Node_already_exists of string
       | Codecs.error ])
      result io
    (** [create ~sep ~dimension_names ~attributes ~codecs ~shape ~chunks kind fill node t]
        creates an array node in store [t] where:
        - Separator [sep] is used in the array's chunk key encoding.
        - Dimension names [dimension_names] and user attributes [attributes]
          are included in it's metadata document.
        - A codec chain defined by [codecs].
        - The array has shape [shape] and chunk shape [chunks].
        - The array has data kind [kind] and fill value [fv]. *)

    val metadata :
      t ->
      Node.Array.t ->
      (Metadata.Array.t, [> `Zarr of error | `Parse_error of string ]) result io
    (** [metadata node t] returns the metadata of array node [node]. *)

    val delete : t -> Node.Array.t -> (unit, [> `Zarr of error]) result io
    (** [delete t n] erases array node [n] from store [t]. If node [n]
        is not a member of store [t] then this is a no-op. *)
    
    val exists : t -> Node.Array.t -> (bool, [> `Zarr of error]) result io
    (** [exists t n] returns [true] if array node [n] is a member
        of store [t] and [false] otherwise. *)

    val write :
      t ->
      Node.Array.t ->
      Ndarray.Indexing.index list ->
      'a Ndarray.t ->
      (unit,
       [> `Zarr of error
       | `Parse_error of string
       | `Invalid_datatype
       | `Invalid_array_slice ])
      result io
    (** [write t n s x] writes n-dimensional array [x] to the slice [s]
        of array node [n] in store [t]. *)

    val read :
      t ->
      Node.Array.t ->
      Ndarray.Indexing.index list ->
      'a Ndarray.dtype ->
      ('a Ndarray.t,
       [> `Zarr of error
       | `Parse_error of string
       | `Invalid_datatype
       | `Invalid_array_slice ])
      result io
    (** [read t n s k] reads an n-dimensional array of size determined
        by slice [s] from array node [n]. *)

    val resize :
      t ->
      Node.Array.t ->
      int list ->
      (unit,
       [> `Zarr of error
       | `Invalid_resize_shape
       | `Parse_error of string ])
      result io
    (** [resize t n shape] resizes array node [n] of store [t] into new
        size [shape]. Note that when the resizing involves shrinking an array
        along any dimensions, any old unreachable chunks that fall outside of
        the array's new shape are deleted from the store. *)

    val rename :
      t ->
      Node.Array.t ->
      string ->
      (Node.Array.t,
       [> `Zarr of error
       | `Key_not_found of string
       | `Node_invariant of string
       | `Cannot_rename_root ])
      result io
    (** [rename t n name] changes the name of array node [n] in store [t] to [name]. *)
  end
end

module type Interface = sig
  (** A Zarr store is a system that can be used to store and retrieve data
      from a Zarr hierarchy. For a store to be compatible with this
      specification, it must support a set of operations defined in the
      Abstract store interface {!STORE}. The store interface can be
      implemented using a variety of underlying storage technologies. *)

  module type S = S
  (** The module interface that all supported stores must implement. *)

  module Make : functor (IO : Types.IO) (Store : Types.Store with type 'a io = 'a IO.t) -> S
    with type error = Store.error
    and type t = Store.t
    and type 'a io := 'a IO.t
  (** A functor for minting a new storage type as long as it's argument
      module implements the {!Store} interface. *)
end
