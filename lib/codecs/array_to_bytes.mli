open Codecs_intf

module ArrayToBytes : sig
  val parse :
    array_tobytes ->
    ('a, 'b) Util.array_repr ->
    (unit, [> error]) result
  val compute_encoded_size : int -> array_tobytes -> int
  val default : array_tobytes
  val encode :
    array_tobytes ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (string, [> error]) result
  val decode :
    array_tobytes ->
    ('a, 'b) Util.array_repr ->
    string ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
    ,[> `Store_read of string | error]) result
  val of_yojson : Yojson.Safe.t -> (array_tobytes, string) result
  val to_yojson : array_tobytes -> Yojson.Safe.t
end

module ShardingIndexedCodec : sig
  type t = internal_shard_config
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
end
