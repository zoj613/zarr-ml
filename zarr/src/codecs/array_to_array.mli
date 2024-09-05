open Codecs_intf

module ArrayToArray : sig 
  val parse : arraytoarray -> int array -> unit
  val encoded_size : int -> arraytoarray -> int
  val encoded_repr : int array -> arraytoarray -> int array
  val encode : 'a Ndarray.t -> arraytoarray -> 'a Ndarray.t
  val decode : arraytoarray -> 'a Ndarray.t -> 'a Ndarray.t
  val of_yojson : int array -> Yojson.Safe.t -> (arraytoarray, string) result
  val to_yojson : arraytoarray -> Yojson.Safe.t
end
