open Codecs_intf

module BytesToBytes : sig
  val parse : bytestobytes -> unit
  val encoded_size : int -> fixed_bytestobytes -> int
  val encode : string -> bytestobytes -> string
  val decode : bytestobytes -> string -> string
  val of_yojson : Yojson.Safe.t -> (bytestobytes, string) result
  val to_yojson : bytestobytes -> Yojson.Safe.t
end
