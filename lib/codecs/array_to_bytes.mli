open Codecs_intf

module ArrayToBytes : sig
  val parse
    : ('a, 'b) Util.array_repr ->
      array_tobytes ->
      (unit, [> error]) result
  val compute_encoded_size : int -> array_tobytes -> int
  val default : array_tobytes
  val encode :
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    array_tobytes ->
    (string, [> error]) result
  val decode :
    string ->
    ('a, 'b) Util.array_repr ->
    array_tobytes ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (array_tobytes, string) result
  val to_yojson : array_tobytes -> Yojson.Safe.t
end
