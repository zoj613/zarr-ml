open Codecs_intf

module ArrayToArray : sig 
  val parse : arraytoarray -> int array -> unit
  val encoded_size : int -> arraytoarray -> int
  val encoded_repr : int array -> arraytoarray -> int array
  val encode :
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    arraytoarray ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
  val decode :
    arraytoarray ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t ->
    ('a, 'b, Bigarray.c_layout) Bigarray.Genarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytoarray, string) result
  val to_yojson : arraytoarray -> Yojson.Safe.t
end
