module Ndarray = Owl.Dense.Ndarray.Generic

type dimension_order = int array

type array_to_array =
  | Transpose of dimension_order

type error =
  [ `Invalid_transpose_order of dimension_order * string ]

module ArrayToArray : sig 
  val parse
    : ('a, 'b) Util.array_repr ->
      array_to_array ->
      (unit, [> error]) result
  val compute_encoded_size : int -> array_to_array -> int
  val compute_encoded_representation
    : array_to_array ->
      ('a, 'b) Util.array_repr ->
      ('a, 'b) Util.array_repr
  val encode
    : array_to_array ->
      ('a, 'b) Ndarray.t ->
      (('a, 'b) Ndarray.t, [> error]) result
  val decode
    : array_to_array ->
      ('a, 'b) Ndarray.t ->
      (('a, 'b) Ndarray.t, [> error]) result
  val of_yojson : Yojson.Safe.t -> (array_to_array, string) result
  val to_yojson : array_to_array -> Yojson.Safe.t
end
