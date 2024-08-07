open Codecs_intf

module ArrayToArray : sig 
  val parse :
    arraytoarray ->
    ('a, 'b) array_repr ->
    (unit, [> error]) result
  val compute_encoded_size : int -> arraytoarray -> int
  val compute_encoded_representation :
    arraytoarray ->
    ('a, 'b) array_repr ->
    (('a, 'b) array_repr, [> error]) result
  val encode :
    arraytoarray ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t, [> error]) result
  val decode :
    arraytoarray ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    (('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (arraytoarray, string) result
  val to_yojson : arraytoarray -> Yojson.Safe.t
end
