module Ndarray = Owl.Dense.Ndarray.Generic

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type fixed = F
type variable = V

type _ bytes_to_bytes =
  | Crc32c : fixed bytes_to_bytes
  | Gzip : compression_level -> variable bytes_to_bytes

type any_bytes_to_bytes =
  | Any : _ bytes_to_bytes -> any_bytes_to_bytes

type error = 
  [ `Gzip of Ezgzip.error ]

module BytesToBytes : sig
  val compute_encoded_size : int -> fixed bytes_to_bytes -> int
  val encode : any_bytes_to_bytes -> string -> (string, [> error]) result
  val decode : any_bytes_to_bytes -> string -> (string, [> error]) result
  val of_yojson : Yojson.Safe.t -> (any_bytes_to_bytes, string) result
  val to_yojson : any_bytes_to_bytes -> Yojson.Safe.t
end
