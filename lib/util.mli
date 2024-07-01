type ('a, 'b) array_repr =
  {kind : ('a, 'b) Bigarray.kind
  ;shape : int array
  ;fill_value : 'a}
(** The type summarizing the decoded/encoded representation of a Zarr array
    or chunk. *)

module ExtPoint : sig
  (** The type representing a JSON extension point metadata configuration. *)

  type 'a t = {name : string ; configuration : 'a}
  val equal : ('a -> 'a -> bool) -> 'a t -> 'a t -> bool
  val of_yojson
    : (Yojson.Safe.t -> ('a, string) result) ->
      Yojson.Safe.t ->
      ('a t, string) result
  val to_yojson : ('a -> Yojson.Safe.t) -> 'a t -> Yojson.Safe.t
end

module Arraytbl : sig include Hashtbl.S with type key = int array end
(** A hashtable with integer array keys. *)

module ArraySet : sig include Set.S with type elt = int array end
(** A hash set of integer array elements. *)

module Result_syntax : sig
  (** Result monad operator syntax. *)

  val ( >>= )
    : ('a, 'e) result -> ('a -> ('b, 'e) result ) -> ('b, 'e) result
  val ( >>| )
    : ('a, 'e) result -> ('a -> 'b) -> ('b, 'e) result
  val ( >>? )
    : ('a, 'e) result -> ('e -> 'f) -> ('a, 'f) result
end

module Indexing : sig
  (** A module housing functions for creating and manipulating indices and
      slices for working with Zarr arrays. *)

  val slice_of_coords
    : int array list -> Owl_types.index array
  (** [slice_of_coords c] takes a list of array coordinates and returns
      a slice corresponding to the coordinates. *)
      
  val coords_of_slice
    : Owl_types.index array -> int array -> int array array
  (** [coords_of_slice s shp] returns an array of coordinates given
      a slice [s] and array shape [shp]. *)

  val cartesian_prod
    : 'a list list -> 'a list list
  (** [cartesian_prod ll] returns a cartesian product of the elements of
      list [ll]. It is mainly used to generate a C-order of chunk indices
      in a regular Zarr array grid. *)

  val slice_shape
    : Owl_types.index array -> int array -> int array
  (** [slice_shape s shp] returns the shape of slice [s] within an array
      of shape [shp]. *)
end

val get_name : Yojson.Safe.t -> string
(** [get_name c] returns the name value of a JSON metadata extension point
    configuration of the form [{"name": value, "configuration": ...}],
    as defined in the Zarr V3 specification. *)

val prod : int array -> int
(** [prod x] returns the product of the elements of [x]. *)
