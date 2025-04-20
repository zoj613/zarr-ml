(** This module provides functionality for manipulating a Zarr node's
    metadata JSON document.

    The Zarr V3 specification defines two types of metadata documents: 
    array and group metadata. Both types are stored under the key
    [zarr.json] within the prefix of a group or array.*)

type error =
  [ `Parse_error of string  (** when parsing a metadata JSON document fails. *)
  | `Invalid_dimension_names  (** when dimension_names array is not the same length as an array's dimensionality. *)
  | `Invalid_datatype ]

module Array : sig
  (** A module which contains functionality to work with a parsed JSON
      Zarr array metadata document. *)

  type t
  (** A type representing a parsed array metadata document. *)

  val create :
    ?sep:[< `Dot | `Slash > `Slash ] ->
    ?attributes:Yojson.Safe.t ->
    ?dimension_names:string option list ->
    codecs:Codecs.Chain.t ->
    shape:int list ->
    'a Ndarray.dtype ->
    'a ->
    int list ->
      (t, [> `Invalid_dimension_names | `Invalid_grid_chunk_shape ]) result
  (** [create ~codecs ~shape kind fv cshp] Creates a new array metadata
      document with codec chain [codecs], shape [shape], fill value [fv],
      data type [kind] and chunk shape [cshp]. *)

  val encode : t -> string
  (** [encode t] returns a byte string representing a JSON Zarr array metadata. *)

  val decode : string -> (t, [> `Parse_error of string ]) result
  (** [decode s] decodes a bytes string [s] into a {!ArrayMetadata.t} type.*)

  val shape : t -> int list
  (** [shape t] returns the shape of the zarr array represented by metadata type [t]. *)

  val chunk_shape : t -> int list
  (** [chunk_shape t] returns the shape a chunk in this zarr array. *)

  val fill_value : t -> 'a Ndarray.dtype -> ('a, [> `Invalid_datatype ]) result
  (** [fill_value t d] returns the fill value used for uninitialized chunks in
      this zarr array given array datatype [d]. *)

  val attributes : t -> Yojson.Safe.t
  (** [attributes t] Returns a Yojson type containing user attributes assigned
      to the zarr array represented by [t]. *)

  val dimension_names : t -> string option list
  (** [dimension_name t] returns a list of dimension names. If none are
      defined then an empty list is returned. *)

  val codecs : t -> Codecs.Chain.t
  (** [codecs t] Returns a type representing the chain of codecs applied
      when decoding/encoding a Zarr array chunk. *)

  val index_coord_pair : t -> int list -> int list * int list
  (** [index_coord_pair t coord] maps a coordinate of this Zarr array to
      a pair of chunk index and coordinate {i within} that chunk. *)

  val chunk_indices : t -> int list -> int list list
  (** [chunk_indices t shp] returns a list of all chunk indices that would
      be contained in a zarr array of shape [shp] given the regular grid
      defined in array metadata [t]. *)

  val chunk_key : t -> int list -> string
  (** [chunk_key t idx] returns a key encoding of a the chunk index [idx]. *)

  val update_attributes : t -> Yojson.Safe.t -> t
  (** [update_attributes t json] returns a new metadata type with an updated
      attribute field containing contents in [json] *)

  val update_shape : t -> int list -> t
  (** [update_shape t new_shp] returns a new metadata type containing
      shape [new_shp]. *)

  val ( = ) : t -> t -> bool
  (** [a = b] returns true if [a] [b] are equal array metadata documents
      and false otherwise. *)
end

module Group : sig
  (** A module which contains functionality to work with a parsed JSON
      Zarr group metadata document. *)

  type t
  (** A type representing a parsed group metadata document. *)

  val default : t
  (** Return a group metadata type with default values for all fields. *)

  val encode : t -> string
  (** [encode t] returns a byte string representing a JSON Zarr group metadata. *)

  val decode : string -> (t, [> `Parse_error of string ]) result
  (** [decode s] decodes a bytes string [s] into a {!t} type. *)

  val update_attributes : t -> Yojson.Safe.t -> t
  (** [update_attributes t json] returns a new metadata type with an updated
      attribute field containing contents in [json]. *)

  val show : t -> string
  (** [show t] pretty-prints the contents of the group metadata type t. *)

  val attributes : t -> Yojson.Safe.t
  (** [attributes t] Returns a Yojson type containing user attributes assigned
      to the zarr group represented by [t]. *)

  val ( = ) : t -> t -> bool
  (** [a = b] returns true if [a] [b] are equal array metadata documents
      and false otherwise. *)
end
