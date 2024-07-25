open Codecs_intf

module ArrayToBytes : sig
  val parse :
    arraytobytes ->
    ('a, 'b) array_repr ->
    (unit, [> error]) result
  val compute_encoded_size : int -> fixed_arraytobytes -> int
  val encode :
    arraytobytes ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (string, [> error]) result
  val decode :
    arraytobytes ->
    ('a, 'b) array_repr ->
    string ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
    ,[> `Store_read of string | error]) result
  val of_yojson : Yojson.Safe.t -> (arraytobytes, string) result
  val to_yojson : arraytobytes -> Yojson.Safe.t
end

module ShardingIndexedCodec : sig
  type t = internal_shard_config
  val partial_encode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    partial_setter ->
    int ->
    ('a, 'b) array_repr ->
    (int array * 'a) list ->
    (unit, 'c) result
  val partial_decode :
    t ->
    ((int * int option) list ->
      (string list, [> `Store_read of string | error ] as 'c) result) ->
    int ->
    ('a, 'b) array_repr ->
    (int * int array) list ->
    ((int * 'a) list, 'c) result
end
