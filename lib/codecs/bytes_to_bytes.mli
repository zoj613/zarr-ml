module Ndarray = Owl.Dense.Ndarray.Generic

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type fixed_bytestobytes =
  [ `Crc32c ]

type variable_bytestobytes =
  [ `Gzip of compression_level ]

type bytestobytes =
  [ fixed_bytestobytes | variable_bytestobytes ]

type error = 
  [ `Gzip of Ezgzip.error ]

module BytesToBytes : sig
  val compute_encoded_size : int -> fixed_bytestobytes -> int
  val encode : bytestobytes -> string -> (string, [> error]) result
  val decode : bytestobytes -> string -> (string, [> error]) result
  val of_yojson : Yojson.Safe.t -> (bytestobytes, string) result
  val to_yojson : bytestobytes -> Yojson.Safe.t
end
