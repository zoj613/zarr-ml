module Ndarray = Owl.Dense.Ndarray.Generic

type compression_level =
  | L0 | L1 | L2 | L3 | L4 | L5 | L6 | L7 | L8 | L9

type bytes_to_bytes =
  | Crc32c
  | Gzip of compression_level

type error = 
  [ `Gzip of Ezgzip.error ]

val pp_bytes_to_bytes : Format.formatter -> bytes_to_bytes -> unit
val show_bytes_to_bytes : bytes_to_bytes -> string

module BytesToBytes : sig
  val compute_encoded_size : int -> bytes_to_bytes -> int
  val encode : bytes_to_bytes -> string -> (string, [> error]) result
  val decode : bytes_to_bytes -> string -> (string, [> error]) result
  val of_yojson : Yojson.Safe.t -> (bytes_to_bytes, string) result
  val to_yojson : bytes_to_bytes -> Yojson.Safe.t
end
