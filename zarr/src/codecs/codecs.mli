(** An array has an associated list of codecs. Each codec specifies a
    bidirectional transform (an encode transform and a decode transform).
    This module contains building blocks for creating and working with
    a chain of codecs. *)

include Codecs_intf.Interface

(** A module containing functions to encode/decode an array chunk using a
    predefined set of codecs. *)
module Chain : sig
  (** A type representing a valid chain of codecs for
      decoding/encoding a Zarr array chunk. *)
  type t

  (** [create s c] returns a type representing a chain of codecs defined by
      chain [c] and chunk shape [s].

      @raise Bytes_to_bytes_invariant
        if [c] contains more than one bytes->bytes codec.
      @raise Invalid_transpose_order
        if [c] contains a transpose codec with invalid order array.
      @raise Invalid_zstd_level
        if [c] contains a Zstd codec whose compression level is invalid.
      @raise Invalid_sharding_chunk_shape
        if [c] contains a shardingindexed codec with an
        incorrect inner chunk shape. *)
  val create : int array -> codec_chain -> t

  (** [encode t x] computes the encoded byte string representation of
      array chunk [x]. *)
  val encode : t -> 'a Ndarray.t -> string

  (** [decode t repr x] decodes the byte string [x] using codec chain [t]
      and decoded representation type [repr]. *)
  val decode : t -> 'a array_repr -> string -> 'a Ndarray.t

  (** [x = y] returns true if chain [x] is equal to chain [y],
      and false otherwise. *)
  val ( = ) : t -> t -> bool

  (** [of_yojson x] returns a code chain of type {!t} from its json object
      representation. *)
  val of_yojson : int array -> Yojson.Safe.t -> (t, string) result

  (** [to_yojson x] returns a json object representation of codec chain [x]. *)
  val to_yojson : t -> Yojson.Safe.t
end

module Make (Io : Types.IO) : sig

  (** [is_just_sharding t] is [true] if the codec chain [t] contains only
      the [sharding_indexed] codec. *)
  val is_just_sharding : Chain.t -> bool

  val partial_encode :
    Chain.t ->
    ((int * int option) list -> string list Io.Deferred.t) ->
    (?append:bool -> (int * string) list -> unit Io.Deferred.t) ->
    int ->
    'a array_repr ->
    (int array * 'a) list ->
    'a ->
    unit Io.Deferred.t

  val partial_decode :
    Chain.t ->
    ((int * int option) list -> string list Io.Deferred.t) ->
    int ->
    'a array_repr ->
    (int * int array) list ->
    'a ->
    (int * 'a) list Io.Deferred.t
end
