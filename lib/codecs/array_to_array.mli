module Ndarray = Owl.Dense.Ndarray.Generic

type arraytoarray =
  [ `Transpose of int array ]

type error =
  [ `Transpose_order of int array * string ]

module ArrayToArray : sig 
  val parse
    : ('a, 'b) Util.array_repr ->
      arraytoarray ->
      (unit, [> error]) result
  val compute_encoded_size : int -> arraytoarray -> int
  val compute_encoded_representation
    : arraytoarray ->
      ('a, 'b) Util.array_repr ->
      (('a, 'b) Util.array_repr, [> error]) result
  val encode
    : arraytoarray ->
      ('a, 'b) Ndarray.t ->
      (('a, 'b) Ndarray.t, [> error]) result
  val decode
    : arraytoarray ->
      ('a, 'b) Ndarray.t ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (arraytoarray, string) result
  val to_yojson : arraytoarray -> Yojson.Safe.t
end
