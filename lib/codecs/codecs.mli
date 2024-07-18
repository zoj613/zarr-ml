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

  (** [create r c] returns a type representing a chain of codecs defined by
      chain [c] and decoded array representation type [r]. *)
  val create :
    ('a, 'b) Util.array_repr -> codec_chain -> (t, [> error ]) result

  (** [default] returns the default codec chain that contains only
      the required codecs as defined in the Zarr Version 3 specification. *)
  val default : t

  (** [is_just_sharding t] is [true] if the codec chain [t] contains only
      the [sharding_indexed] codec. *)
  val is_just_sharding : t -> bool

  (** [encode t x] computes the encoded byte string representation of
      array chunk [x]. Returns an error upon failure. *)
  val encode :
    t ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (string, [> error ]) result

  (** [decode t repr x] decodes the byte string [x] using codec chain [t]
      and decoded representation type [repr]. Returns an error upon failure.*)
  val decode :
    t ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
    ,[> `Store_read of string | error ]) result

  val partial_encode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    partial_setter ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int array * 'a) list ->
    (unit, 'c) result

  val partial_decode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    int ->
    ('a, 'b) Util.array_repr ->
    (int * int array) list ->
    ((int * 'a) list, 'c) result

  (** [x = y] returns true if chain [x] is equal to chain [y],
      and false otherwise. *)
  val ( = ) : t -> t -> bool

  (** [of_yojson x] returns a code chain of type {!t} from its json object
      representation. *)
  val of_yojson : Yojson.Safe.t -> (t, string) result

  (** [to_yojson x] returns a json object representation of codec chain [x]. *)
  val to_yojson : t -> Yojson.Safe.t
end
