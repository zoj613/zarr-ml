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
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (array_tobytes, string) result
  val to_yojson : array_tobytes -> Yojson.Safe.t
end
