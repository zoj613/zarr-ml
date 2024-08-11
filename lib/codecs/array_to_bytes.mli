open Codecs_intf

module ArrayToBytes : sig
  val parse : arraytobytes -> int array -> unit
  val compute_encoded_size : int -> fixed_arraytobytes -> int
  val encode :
    arraytobytes ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    string
  val decode :
    arraytobytes ->
    ('a, 'b) array_repr ->
    string ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end

module ShardingIndexedCodec : sig
  type t = internal_shard_config
  val partial_encode :
    t ->
    ((int * int option) list -> string list) ->
    partial_setter ->
    int ->
    ('a, 'b) array_repr ->
    (int array * 'a) list ->
    unit
  val partial_decode :
    t ->
    ((int * int option) list -> string list) ->
    int ->
    ('a, 'b) array_repr ->
    (int * int array) list ->
    (int * 'a) list
end
