open Codecs_intf

module Ndarray = Owl.Dense.Ndarray.Generic

module BytesToBytes : sig
  val compute_encoded_size : int -> fixed_bytestobytes -> int
  val encode : bytestobytes -> string -> (string, [> error]) result
  val decode : bytestobytes -> string -> (string, [> error]) result
  val of_yojson : Yojson.Safe.t -> (bytestobytes, string) result
  val to_yojson : bytestobytes -> Yojson.Safe.t
end
